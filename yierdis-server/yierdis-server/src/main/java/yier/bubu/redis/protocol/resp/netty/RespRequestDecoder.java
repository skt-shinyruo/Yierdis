package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import yier.bubu.redis.bytes.BytesView;
import yier.bubu.redis.execution.api.ByteArrayExecutionRequest;
import yier.bubu.redis.execution.api.ReferenceCountedRequestMemoryLease;
import yier.bubu.redis.execution.api.RequestMemoryLease;
import yier.bubu.redis.protocol.resp.InlineCommandParser;
import yier.bubu.redis.protocol.resp.RespProtocolLimits;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * RESP 请求解码器。所有可控的数组与 bulk 分配在创建前都经过入站预算准入。
 */
public final class RespRequestDecoder extends ChannelInboundHandlerAdapter {
    private static final byte CR = (byte) '\r';
    private static final byte LF = (byte) '\n';
    private static final byte ARRAY = (byte) '*';
    private static final byte BULK = (byte) '$';
    private static final int REQUEST_FIXED_BYTES = 32;
    private static final int OUTER_ARGV_BYTES = 16;
    private static final int REFERENCE_BYTES = 8;
    private static final int ARRAY_HEADER_BYTES = 16;
    private static final long INLINE_DECODED_OBJECT_BYTES = 32L;
    private static final long INLINE_PARSER_TRANSIENT_FLOOR_BYTES = 512L;
    private static final int MAX_COMPONENTS = 16;
    private static final int INCOMPLETE_LINE = Integer.MIN_VALUE;
    private static final int INVALID_LINE = -1;

    private final int maxBulkBytes;
    private final int maxArgs;
    private final int maxInlineBytes;
    private final int maxCommandBytes;
    private final InboundMemoryBudget budget;
    private final InboundConnectionMemory connection;
    private final RespDecodedMessageGate decodedMessageGate;
    private final ByteBufLineView inlineLineView = new ByteBufLineView();

    private DecodePhase phase = SimplePhase.READ_COMMAND;
    private AccountedRespCumulator cumulator;
    private InboundReadControl readControl = InboundReadControl.NOOP;
    private int allocatedArgvArrays;
    private int allocatedBulkArrays;

    RespRequestDecoder(int maxBulkBytes, int maxArgs, int maxInlineBytes, int maxCommandBytes) {
        this(maxBulkBytes, maxArgs, maxInlineBytes, maxCommandBytes, null, null, RespDecodedMessageGate.PASS_THROUGH);
    }

    public static RespRequestDecoder withIngressAdmission(
            int maxBulkBytes,
            int maxArgs,
            int maxInlineBytes,
            int maxCommandBytes,
            InboundMemoryBudget budget,
            InboundConnectionMemory connection,
            RespDecodedMessageGate decodedMessageGate
    ) {
        return new RespRequestDecoder(
                maxBulkBytes,
                maxArgs,
                maxInlineBytes,
                maxCommandBytes,
                budget,
                connection,
                decodedMessageGate
        );
    }

    private RespRequestDecoder(
            int maxBulkBytes,
            int maxArgs,
            int maxInlineBytes,
            int maxCommandBytes,
            InboundMemoryBudget budget,
            InboundConnectionMemory connection,
            RespDecodedMessageGate decodedMessageGate
    ) {
        if ((budget == null) != (connection == null)) {
            throw new IllegalArgumentException("budget and connection must be supplied together");
        }
        this.maxBulkBytes = Math.max(0, maxBulkBytes);
        this.maxArgs = Math.max(0, maxArgs);
        this.maxInlineBytes = Math.max(0, maxInlineBytes);
        this.maxCommandBytes = Math.max(0, maxCommandBytes);
        this.budget = budget;
        this.connection = connection;
        this.decodedMessageGate = decodedMessageGate == null ? RespDecodedMessageGate.PASS_THROUGH : decodedMessageGate;
    }

    @Override
    public void handlerAdded(ChannelHandlerContext ctx) {
        ensureCumulator(ctx);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!phase.acceptsInput()) {
            releaseIncoming(msg);
            return;
        }
        if (msg instanceof RespProtocolError error) {
            emitProtocolError(ctx, error.message());
            return;
        }
        ensureCumulator(ctx);
        if (msg instanceof AccountedInboundBuffer accounted) {
            cumulator.append(accounted.takeBuffer(), accounted.takeLease());
        } else if (msg instanceof ByteBuf input) {
            InboundBufferLease lease = admitRawInput(ctx, input);
            if (lease == null) {
                ReferenceCountUtil.safeRelease(input);
                return;
            }
            cumulator.append(input, lease);
        } else {
            ctx.fireChannelRead(msg);
            return;
        }
        process(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        cleanup();
        super.channelInactive(ctx);
    }

    @Override
    public void handlerRemoved(ChannelHandlerContext ctx) throws Exception {
        cleanup();
        super.handlerRemoved(ctx);
    }

    public void setReadControl(InboundReadControl readControl) {
        this.readControl = readControl == null ? InboundReadControl.NOOP : readControl;
    }

    int allocatedArgvArraysForTests() {
        return allocatedArgvArrays;
    }

    int allocatedBulkArraysForTests() {
        return allocatedBulkArrays;
    }

    String stateNameForTests() {
        return phase.stateName();
    }

    private void ensureCumulator(ChannelHandlerContext ctx) {
        if (cumulator == null) {
            cumulator = new AccountedRespCumulator(ctx.alloc(), budget, connection, MAX_COMPONENTS);
        }
    }

    private InboundBufferLease admitRawInput(ChannelHandlerContext ctx, ByteBuf input) {
        if (budget == null) {
            return InboundBufferLease.unaccounted();
        }
        long charge = InboundBufferLease.chargeForRetainedBuffer(input);
        InboundMemoryBudget.ReservationResult result = budget.tryReserve(connection, charge);
        if (result != InboundMemoryBudget.ReservationResult.RESERVED) {
            emitRequestMemoryError(ctx);
            return null;
        }
        return InboundBufferLease.admitted(budget, connection.account(), charge);
    }

    private void process(ChannelHandlerContext ctx) {
        if (phase == SimplePhase.CLOSING || cumulator == null) {
            return;
        }
        // 先恢复 decoder 自己的唯一 pending phase，避免同一连接又进入 consolidation admission。
        if (!resumePendingPhase(ctx) || phase == SimplePhase.CLOSING) {
            return;
        }
        if (!completeConsolidation(ctx)) {
            return;
        }

        ByteBuf in = cumulator.buffer();
        while (phase != SimplePhase.CLOSING && in.isReadable()) {
            ParseResult result;
            if (phase == SimplePhase.READ_COMMAND) {
                byte first = in.getByte(in.readerIndex());
                result = first == ARRAY ? tryStartArray(ctx, in) : tryReadInline(ctx, in);
            } else if (phase instanceof ArrayPhase
                    || phase instanceof BulkReadyPhase
                    || phase instanceof BulkBodyPhase) {
                result = tryContinueArray(ctx, in);
            } else {
                return;
            }

            if (result == ParseResult.NEED_MORE) {
                cumulator.discardFullyReadComponents();
                readControl.resumeIngressForProgress();
                return;
            }
            if (result == ParseResult.WAITING || result == ParseResult.ERROR) {
                return;
            }
            if (!completeConsolidation(ctx)) {
                return;
            }
            if (!resumePendingPhase(ctx)) {
                return;
            }
        }
        cumulator.discardFullyReadComponents();
        if (phase != SimplePhase.CLOSING) {
            readControl.resumeIngress();
        }
    }

    private boolean completeConsolidation(ChannelHandlerContext ctx) {
        AccountedRespCumulator.ConsolidationResult result = cumulator.consolidateIfNeeded(
                ctx.executor(),
                () -> resumeOnEventLoop(ctx)
        );
        if (result == AccountedRespCumulator.ConsolidationResult.NOT_NEEDED
                || result == AccountedRespCumulator.ConsolidationResult.CONSOLIDATED) {
            return true;
        }
        if (result == AccountedRespCumulator.ConsolidationResult.REQUEST_LIMIT) {
            emitRequestMemoryError(ctx);
            return false;
        }
        readControl.pauseIngress();
        return false;
    }

    private boolean resumePendingPhase(ChannelHandlerContext ctx) {
        return switch (phase) {
            case ArgvAdmissionPhase pending -> resumeArgvAdmission(ctx, pending);
            case BulkAdmissionPhase pending -> resumeBulkAdmission(ctx, pending);
            case InlineInspectionPhase pending -> resumeInlineInspection(ctx, pending);
            case InlineAdmissionPhase pending -> resumeInlineAdmission(ctx, pending);
            case RequestAdmissionPhase pending -> resumeRequestAdmission(ctx, pending);
            case HandoffPhase pending -> forwardPendingDecodedMessage(ctx, pending);
            default -> true;
        };
    }

    private ParseResult tryStartArray(ChannelHandlerContext ctx, ByteBuf in) {
        int lineStart = in.readerIndex();
        int lf = findCrlfLine(ctx, in, "ERR Protocol error: invalid multibulk length");
        if (lf == INCOMPLETE_LINE) {
            return ParseResult.NEED_MORE;
        }
        if (lf == INVALID_LINE) {
            return ParseResult.ERROR;
        }

        Long argcValue = parseInteger(in, lineStart + 1, lf - 1);
        if (argcValue == null || argcValue < 0 || argcValue > RespProtocolLimits.DEFAULT_MAX_ARGS) {
            emitProtocolError(ctx, "ERR Protocol error: invalid multibulk length");
            return ParseResult.ERROR;
        }
        int argc = argcValue.intValue();
        if (maxArgs > 0 && argc > maxArgs) {
            emitProtocolError(ctx, "ERR Protocol error: too many arguments");
            return ParseResult.ERROR;
        }

        PendingAdmission admission = progressAdmission(
                outerArgvCharge(argc),
                cumulator.releasableChargeAfterRead(0)
        );
        ArgvAdmissionPhase pending = new ArgvAdmissionPhase(argc, admission);
        if (!requestAdmission(ctx, pending)) {
            return phase == SimplePhase.CLOSING ? ParseResult.ERROR : ParseResult.WAITING;
        }
        if (!resumeArgvAdmission(ctx, pending)) {
            return ParseResult.ERROR;
        }
        return ParseResult.CONTINUE;
    }

    private boolean resumeArgvAdmission(ChannelHandlerContext ctx, ArgvAdmissionPhase pending) {
        if (!consumeAdmission(pending.admission())) {
            return false;
        }
        long reservedBytes = pending.admission().bytes;
        byte[][] argv;
        try {
            argv = new byte[pending.argc()][];
        } catch (OutOfMemoryError failure) {
            emitRequestMemoryError(ctx);
            return false;
        }
        allocatedArgvArrays++;
        phase = new ArrayPhase(new ArrayProgress(argv, reservedBytes));
        cumulator.discardFullyReadComponents();
        return true;
    }

    private ParseResult tryContinueArray(ChannelHandlerContext ctx, ByteBuf in) {
        ArrayProgress array;
        BulkBodyPhase bulk;
        if (phase instanceof BulkBodyPhase current) {
            array = current.array();
            bulk = current;
        } else if (phase instanceof BulkReadyPhase current) {
            array = current.array();
            if (!allocateBulkBody(ctx, current)) {
                return ParseResult.ERROR;
            }
            bulk = (BulkBodyPhase) phase;
        } else {
            array = ((ArrayPhase) phase).array();
            bulk = null;
        }
        while (array.argIndex < array.argv.length) {
            if (bulk == null) {
                if (!in.isReadable()) {
                    return ParseResult.NEED_MORE;
                }
                int bulkLineStart = in.readerIndex();
                int bulkLf = findCrlfLine(ctx, in, "ERR Protocol error: invalid bulk length");
                if (bulkLf == INCOMPLETE_LINE) {
                    return ParseResult.NEED_MORE;
                }
                if (bulkLf == INVALID_LINE) {
                    return ParseResult.ERROR;
                }
                if (in.getByte(bulkLineStart) != BULK) {
                    emitProtocolError(ctx, "ERR Protocol error: expected '$', got other");
                    return ParseResult.ERROR;
                }

                Long lenValue = parseInteger(in, bulkLineStart + 1, bulkLf - 1);
                if (lenValue == null || lenValue < -1 || lenValue > RespProtocolLimits.DEFAULT_MAX_BULK_BYTES) {
                    emitProtocolError(ctx, "ERR Protocol error: invalid bulk length");
                    return ParseResult.ERROR;
                }
                if (lenValue == -1L) {
                    array.argv[array.argIndex++] = null;
                    cumulator.discardFullyReadComponents();
                    continue;
                }
                int length = lenValue.intValue();
                if (maxBulkBytes > 0 && length > maxBulkBytes) {
                    emitProtocolError(ctx, "ERR Protocol error: invalid bulk length");
                    return ParseResult.ERROR;
                }
                if (maxCommandBytes > 0 && array.retainedBytes > maxCommandBytes - length) {
                    emitProtocolError(ctx, "ERR Protocol error: command is too large");
                    return ParseResult.ERROR;
                }
                PendingAdmission admission = progressAdmission(
                        payloadCharge(length),
                        cumulator.releasableChargeAfterRead(0)
                );
                BulkAdmissionPhase pending = new BulkAdmissionPhase(array, length, admission);
                if (!requestAdmission(ctx, pending)) {
                    return phase == SimplePhase.CLOSING ? ParseResult.ERROR : ParseResult.WAITING;
                }
                if (!resumeBulkAdmission(ctx, pending)) {
                    return ParseResult.ERROR;
                }
                BulkReadyPhase ready = (BulkReadyPhase) phase;
                if (!allocateBulkBody(ctx, ready)) {
                    return ParseResult.ERROR;
                }
                bulk = (BulkBodyPhase) phase;
            }
            int remainingBody = bulk.length() - bulk.bytesRead;
            if (remainingBody > 0 && in.isReadable()) {
                int copied = Math.min(remainingBody, in.readableBytes());
                in.readBytes(bulk.buffer(), bulk.bytesRead, copied);
                bulk.bytesRead += copied;
                cumulator.discardFullyReadComponents();
            }
            if (bulk.bytesRead < bulk.length()) {
                return ParseResult.NEED_MORE;
            }

            if (in.readableBytes() < 2L) {
                return ParseResult.NEED_MORE;
            }

            byte cr = in.readByte();
            byte lf = in.readByte();
            if (cr != CR || lf != LF) {
                emitProtocolError(ctx, "ERR Protocol error: invalid bulk string terminator");
                return ParseResult.ERROR;
            }
            array.argv[array.argIndex++] = bulk.buffer();
            array.retainedBytes = saturatedAdd(array.retainedBytes, bulk.length());
            phase = new ArrayPhase(array);
            bulk = null;
            cumulator.discardFullyReadComponents();
        }

        return completeRequest(ctx, array);
    }

    private boolean resumeBulkAdmission(ChannelHandlerContext ctx, BulkAdmissionPhase pending) {
        if (!consumeAdmission(pending.admission())) {
            return false;
        }
        ArrayProgress array = pending.array();
        array.reservedBytes = InboundMemoryBudget.saturatedAdd(
                array.reservedBytes,
                pending.admission().bytes
        );
        phase = new BulkReadyPhase(array, pending.length());
        return true;
    }

    private boolean allocateBulkBody(ChannelHandlerContext ctx, BulkReadyPhase ready) {
        ArrayProgress array = ready.array();
        byte[] buffer;
        try {
            buffer = new byte[ready.length()];
        } catch (OutOfMemoryError failure) {
            emitRequestMemoryError(ctx);
            return false;
        }
        allocatedBulkArrays++;
        phase = new BulkBodyPhase(array, ready.length(), buffer);
        cumulator.discardFullyReadComponents();
        return true;
    }

    private ParseResult completeRequest(ChannelHandlerContext ctx, ArrayProgress array) {
        PendingAdmission admission = progressAdmission(
                REQUEST_FIXED_BYTES,
                cumulator.releasableChargeAfterRead(0)
        );
        RequestAdmissionPhase pending = new RequestAdmissionPhase(array, admission);
        if (!requestAdmission(ctx, pending)) {
            return phase == SimplePhase.CLOSING ? ParseResult.ERROR : ParseResult.WAITING;
        }
        return resumeRequestAdmission(ctx, pending) ? ParseResult.CONTINUE : ParseResult.WAITING;
    }

    private boolean resumeRequestAdmission(ChannelHandlerContext ctx, RequestAdmissionPhase pending) {
        if (!consumeAdmission(pending.admission())) {
            return false;
        }
        ArrayProgress array = pending.array();
        array.reservedBytes = InboundMemoryBudget.saturatedAdd(
                array.reservedBytes,
                pending.admission().bytes
        );
        RespDecodedMessage decoded = buildCompletedRequest(array);
        HandoffPhase handoff = new HandoffPhase(decoded);
        phase = handoff;
        return forwardPendingDecodedMessage(ctx, handoff);
    }

    private RespDecodedMessage buildCompletedRequest(ArrayProgress array) {
        RequestMemoryLease lease = budget == null
                ? new ReferenceCountedRequestMemoryLease(array.reservedBytes, ignored -> { })
                : requestLease(array.reservedBytes);
        return new RespDecodedMessage.Request(
                ByteArrayExecutionRequest.takeOwnership(array.argv, array.retainedBytes, lease)
        );
    }

    private boolean forwardPendingDecodedMessage(ChannelHandlerContext ctx, HandoffPhase handoff) {
        RespDecodedMessage decoded = handoff.message();
        if (handoff.terminal()) {
            readControl.pauseIngress();
        }
        HandoffAttempt attempt = new HandoffAttempt();
        RespDecodedMessageGate.Admission admission;
        handoff.admissionInProgress = true;
        try {
            admission = decodedMessageGate.tryAdmit(
                    ctx,
                    decoded,
                    () -> resumeHandoffLater(ctx, handoff, attempt)
            );
        } catch (Throwable ignored) {
            handoff.admissionInProgress = false;
            decoded.close();
            enterClosing();
            return false;
        }
        handoff.admissionInProgress = false;
        if (admission == null) {
            decoded.close();
            enterClosing();
            return false;
        }
        if (phase != handoff) {
            if (admission != RespDecodedMessageGate.Admission.ADMITTED) {
                decoded.close();
            }
            return false;
        }
        return switch (admission) {
            case ADMITTED -> {
                phase = handoff.terminal() ? SimplePhase.CLOSING : SimplePhase.READ_COMMAND;
                if (!handoff.terminal()) {
                    readControl.resumeIngress();
                }
                yield !handoff.terminal();
            }
            case WAITING -> {
                phase = handoff;
                readControl.pauseIngress();
                yield false;
            }
            case CLOSED -> {
                decoded.close();
                enterClosing();
                yield false;
            }
        };
    }

    private ParseResult tryReadInline(ChannelHandlerContext ctx, ByteBuf in) {
        int lineStart = in.readerIndex();
        int lf = findCrlfLine(ctx, in, "ERR Protocol error: invalid inline command");
        if (lf == INCOMPLETE_LINE) {
            return ParseResult.NEED_MORE;
        }
        if (lf == INVALID_LINE) {
            return ParseResult.ERROR;
        }
        int lineEnd = lf - 1;
        int length = lineEnd - lineStart;
        boolean blank = InlineCommandParser.isBlank(inlineLineView.reset(in, lineStart, length));
        inlineLineView.clear();
        if (blank) {
            cumulator.discardFullyReadComponents();
            return ParseResult.CONTINUE;
        }
        if (maxCommandBytes > 0 && length > maxCommandBytes) {
            emitProtocolError(ctx, "ERR Protocol error: command is too large");
            return ParseResult.ERROR;
        }

        long inspectionBytes = inlineInspectionCharge(length);
        long inputReleaseCredit = cumulator.releasableChargeAfterRead(0);
        PendingAdmission admission = progressAdmission(inspectionBytes, inputReleaseCredit);
        InlineInspectionPhase pending = new InlineInspectionPhase(length, admission);
        in.readerIndex(lineStart);
        if (!requestAdmission(ctx, pending)) {
            return phase == SimplePhase.CLOSING ? ParseResult.ERROR : ParseResult.WAITING;
        }
        if (resumeInlineInspection(ctx, pending)) {
            return ParseResult.CONTINUE;
        }
        return phase == SimplePhase.CLOSING ? ParseResult.ERROR : ParseResult.WAITING;
    }

    private boolean resumeInlineInspection(ChannelHandlerContext ctx, InlineInspectionPhase pending) {
        if (!consumeAdmission(pending.admission())) {
            return false;
        }
        ByteBuf in = cumulator.buffer();
        int lineStart = in.readerIndex();
        int length = pending.length();
        in.readerIndex(lineStart + length + 2);
        byte[] line = new byte[length];
        in.getBytes(lineStart, line);
        cumulator.discardFullyReadComponents();
        InlineCommandParser.Parsed parsed;
        try {
            parsed = InlineCommandParser.parseResult(line, 0, line.length);
        } catch (IllegalArgumentException e) {
            emitProtocolError(ctx, "ERR Protocol error: invalid inline command");
            return false;
        }
        if (parsed.argc() == 0) {
            releaseReservedBytes(pending.admission().bytes);
            phase = SimplePhase.READ_COMMAND;
            return true;
        }
        if (maxArgs > 0 && parsed.argc() > maxArgs) {
            emitProtocolError(ctx, "ERR Protocol error: too many arguments");
            return false;
        }
        if (maxCommandBytes > 0 && parsed.retainedBytes() > maxCommandBytes) {
            emitProtocolError(ctx, "ERR Protocol error: command is too large");
            return false;
        }

        long total = inlineAdmissionCharge(length, parsed.argc(), parsed.retainedBytes());
        InlineAdmissionPhase next = new InlineAdmissionPhase(
                parsed,
                pending.admission().bytes,
                progressAdmission(total - pending.admission().bytes, 0L)
        );
        if (!requestAdmission(ctx, next)) {
            return false;
        }
        return resumeInlineAdmission(ctx, next);
    }

    private boolean resumeInlineAdmission(ChannelHandlerContext ctx, InlineAdmissionPhase pending) {
        if (!consumeAdmission(pending.admission())) {
            return false;
        }
        pending.reservedBytes = InboundMemoryBudget.saturatedAdd(
                pending.reservedBytes,
                pending.admission().bytes
        );
        InlineCommandParser.Parsed parsed = pending.parsed();
        byte[][] argv = parsed.takeArgs();
        allocatedArgvArrays++;
        long fullCharge = ByteArrayExecutionRequest.estimatedMemoryBytes(argv);
        if (fullCharge > pending.reservedBytes) {
            emitRequestMemoryError(ctx);
            return false;
        }
        releaseInlineTransient(pending.reservedBytes, fullCharge);
        pending.reservedBytes = fullCharge;
        RequestMemoryLease lease = requestLease(fullCharge);
        RespDecodedMessage decoded = new RespDecodedMessage.Request(
                ByteArrayExecutionRequest.takeOwnership(argv, parsed.retainedBytes(), lease)
        );
        HandoffPhase handoff = new HandoffPhase(decoded);
        phase = handoff;
        return forwardPendingDecodedMessage(ctx, handoff);
    }

    private static PendingAdmission progressAdmission(long bytes, long inputReleaseCredit) {
        return new PendingAdmission(bytes, inputReleaseCredit);
    }

    private boolean requestAdmission(ChannelHandlerContext ctx, AdmissionPhase requestedPhase) {
        PendingAdmission requested = requestedPhase.admission();
        phase = requestedPhase;
        if (requested.bytes < 0L || requested.inputReleaseCredit < 0L) {
            emitRequestMemoryError(ctx);
            return false;
        }
        if (budget == null) {
            requested.granted = true;
            return true;
        }
        connection.setResumeCallback(ctx.executor(), () -> {
            if (phase == requestedPhase && connection.claimGrantedReservation(requested.bytes)) {
                requested.granted = true;
                resumeOnEventLoop(ctx);
            }
        });
        InboundMemoryBudget.ReservationResult result = budget.tryTransferForProgress(
                connection,
                requested.bytes,
                requested.inputReleaseCredit
        );
        if (result == InboundMemoryBudget.ReservationResult.RESERVED) {
            requested.granted = true;
            return true;
        }
        if (result == InboundMemoryBudget.ReservationResult.WAITING) {
            readControl.pauseIngress();
            return false;
        }
        emitRequestMemoryError(ctx);
        return false;
    }

    private static boolean consumeAdmission(PendingAdmission admission) {
        if (!admission.granted) {
            return false;
        }
        admission.consumed = true;
        return true;
    }

    private void resumeOnEventLoop(ChannelHandlerContext ctx) {
        if (ctx.executor().inEventLoop()) {
            process(ctx);
            return;
        }
        ctx.executor().execute(() -> process(ctx));
    }

    private void resumeHandoffLater(
            ChannelHandlerContext ctx,
            HandoffPhase handoff,
            HandoffAttempt attempt
    ) {
        if (!attempt.scheduled.compareAndSet(false, true)) {
            return;
        }
        try {
            // gate 可能同步触发 wake-up；延后到 event loop，避免 tryAdmit 返回前重入同一 handoff。
            ctx.executor().execute(() -> {
                if (phase == handoff) {
                    process(ctx);
                }
            });
        } catch (RuntimeException ignored) {
            ctx.close();
        }
    }

    private int findCrlfLine(ChannelHandlerContext ctx, ByteBuf in, String errorMessage) {
        int start = in.readerIndex();
        int lfDistance = in.bytesBefore(LF);
        if (lfDistance < 0) {
            if (maxInlineBytes > 0 && in.readableBytes() > maxInlineBytes) {
                in.readerIndex(in.writerIndex());
                emitProtocolError(ctx, errorMessage);
                return INVALID_LINE;
            }
            return INCOMPLETE_LINE;
        }
        if (maxInlineBytes > 0 && lfDistance + 1 > maxInlineBytes) {
            in.readerIndex(start + lfDistance + 1);
            emitProtocolError(ctx, errorMessage);
            return INVALID_LINE;
        }
        int lf = start + lfDistance;
        if (lf == start || in.getByte(lf - 1) != CR) {
            in.readerIndex(lf + 1);
            emitProtocolError(ctx, errorMessage);
            return INVALID_LINE;
        }
        in.readerIndex(lf + 1);
        return lf;
    }

    private void emitRequestMemoryError(ChannelHandlerContext ctx) {
        emitProtocolError(ctx, "ERR request exceeds configured memory limit");
    }

    private void emitProtocolError(ChannelHandlerContext ctx, String message) {
        releasePhase();
        enterClosing();
        if (cumulator != null) {
            cumulator.close();
        }
        HandoffPhase handoff = new HandoffPhase(new RespProtocolError(message));
        phase = handoff;
        forwardPendingDecodedMessage(ctx, handoff);
    }

    private void enterClosing() {
        phase = SimplePhase.CLOSING;
        readControl.pauseIngress();
    }

    private void cleanup() {
        releasePhase();
        phase = SimplePhase.CLOSING;
        if (cumulator != null) {
            cumulator.close();
            cumulator = null;
        }
        readControl.pauseIngress();
    }

    private void releasePhase() {
        DecodePhase current = phase;
        // 先摘下 phase，释放预算时触发的同步回调就无法再次消费同一份所有权。
        phase = SimplePhase.READ_COMMAND;
        if (current instanceof AdmissionPhase admissionPhase) {
            releasePendingAdmission(admissionPhase.admission());
        }
        releaseReservedBytes(current.reservedBytes());
        // 同步下游可在 tryAdmit 内移除 decoder；等 admission 返回后再判定消息由谁释放。
        if (current instanceof HandoffPhase handoff && !handoff.admissionInProgress) {
            handoff.message().close();
        }
    }

    private void releasePendingAdmission(PendingAdmission pending) {
        if (pending.consumed || budget == null || connection == null) {
            return;
        }
        if (pending.granted || connection.claimGrantedReservation(pending.bytes)) {
            budget.release(connection, pending.bytes);
        } else {
            budget.cancelWaiter(connection);
        }
    }

    private void releaseReservedBytes(long reservedBytes) {
        if (reservedBytes > 0L && budget != null) {
            budget.release(connection.account(), reservedBytes);
        }
    }

    private static void releaseIncoming(Object msg) {
        if (msg instanceof AccountedInboundBuffer accounted) {
            accounted.close();
            return;
        }
        ReferenceCountUtil.safeRelease(msg);
    }

    private static long outerArgvCharge(int argc) {
        return InboundMemoryBudget.saturatedAdd(OUTER_ARGV_BYTES,
                saturatedMultiply(Math.max(0, argc), REFERENCE_BYTES));
    }

    private RequestMemoryLease requestLease(long reservedBytes) {
        if (budget == null) {
            return new ReferenceCountedRequestMemoryLease(reservedBytes, ignored -> { });
        }
        InboundMemoryBudget ownerBudget = budget;
        ConnectionMemoryAccount account = connection.account();
        return new ReferenceCountedRequestMemoryLease(reservedBytes, bytes -> ownerBudget.release(account, bytes));
    }

    private void releaseInlineTransient(long admittedBytes, long retainedBytes) {
        long transientBytes = admittedBytes - retainedBytes;
        if (transientBytes > 0L && budget != null) {
            budget.release(connection.account(), transientBytes);
        }
    }

    private static long inlineInspectionCharge(int lineLength) {
        long lineArrays = InboundMemoryBudget.saturatedAdd(payloadCharge(lineLength), payloadCharge(lineLength));
        return InboundMemoryBudget.saturatedAdd(lineArrays, INLINE_DECODED_OBJECT_BYTES);
    }

    private static long inlineAdmissionCharge(int lineLength, int argc, int retainedBytes) {
        long finalRequestCharge = inlineFinalChargeUpperBound(argc, retainedBytes);
        long parserTransient = InboundMemoryBudget.saturatedAdd(payloadCharge(lineLength), payloadCharge(lineLength));
        parserTransient = InboundMemoryBudget.saturatedAdd(parserTransient, INLINE_DECODED_OBJECT_BYTES);
        parserTransient = Math.max(parserTransient, INLINE_PARSER_TRANSIENT_FLOOR_BYTES);
        return InboundMemoryBudget.saturatedAdd(finalRequestCharge, parserTransient);
    }

    private static long inlineFinalChargeUpperBound(int argc, int retainedBytes) {
        long total = InboundMemoryBudget.saturatedAdd(REQUEST_FIXED_BYTES, outerArgvCharge(argc));
        long perArgumentHeaders = saturatedMultiply(Math.max(0, argc), ARRAY_HEADER_BYTES);
        long alignmentSlack = saturatedMultiply(Math.max(0, argc), 7L);
        total = InboundMemoryBudget.saturatedAdd(total, perArgumentHeaders);
        total = InboundMemoryBudget.saturatedAdd(total, Math.max(0, retainedBytes));
        return InboundMemoryBudget.saturatedAdd(total, alignmentSlack);
    }

    private static long payloadCharge(int length) {
        return InboundMemoryBudget.saturatedAdd(16L, align8(length));
    }

    private static long align8(int length) {
        return align8((long) Math.max(0, length));
    }

    private static long align8(long length) {
        if (length <= 0L) {
            return 0L;
        }
        return length > Long.MAX_VALUE - 7L ? Long.MAX_VALUE : (length + 7L) & ~7L;
    }

    private static long saturatedMultiply(long left, long right) {
        if (left <= 0L || right <= 0L) {
            return 0L;
        }
        return left > Long.MAX_VALUE / right ? Long.MAX_VALUE : left * right;
    }

    private static final class ByteBufLineView implements BytesView {
        private ByteBuf buffer;
        private int start;
        private int length;

        private ByteBufLineView reset(ByteBuf buffer, int start, int length) {
            this.buffer = buffer;
            this.start = start;
            this.length = length;
            return this;
        }

        private void clear() {
            buffer = null;
            start = 0;
            length = 0;
        }

        @Override
        public int length() {
            return length;
        }

        @Override
        public byte getByte(int index) {
            if (index < 0 || index >= length) {
                throw new IndexOutOfBoundsException();
            }
            return buffer.getByte(start + index);
        }
    }

    private static Long parseInteger(ByteBuf in, int start, int endExclusive) {
        if (start >= endExclusive) {
            return null;
        }
        boolean negative = false;
        int i = start;
        if (in.getByte(i) == '-') {
            negative = true;
            i++;
            if (i >= endExclusive) {
                return null;
            }
        }
        long value = 0L;
        for (; i < endExclusive; i++) {
            int ch = in.getByte(i) & 0xFF;
            if (ch < '0' || ch > '9') {
                return null;
            }
            value = value * 10L + (ch - '0');
            if (value > Integer.MAX_VALUE) {
                return null;
            }
        }
        return negative ? -value : value;
    }

    private static int saturatedAdd(int current, int value) {
        long next = (long) Math.max(0, current) + Math.max(0, value);
        return next >= Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) next;
    }

    private enum ParseResult {
        NEED_MORE,
        CONTINUE,
        WAITING,
        ERROR
    }

    /** 当前 phase 独占恢复解码所需的数据、预算和 handoff 消息。 */
    private sealed interface DecodePhase
            permits SimplePhase, AdmissionPhase, ArrayPhase, BulkReadyPhase, BulkBodyPhase, HandoffPhase {
        String stateName();

        default boolean acceptsInput() {
            return true;
        }

        default long reservedBytes() {
            return 0L;
        }
    }

    private enum SimplePhase implements DecodePhase {
        READ_COMMAND,
        CLOSING;

        @Override
        public String stateName() {
            return name();
        }

        @Override
        public boolean acceptsInput() {
            return this != CLOSING;
        }
    }

    private sealed interface AdmissionPhase extends DecodePhase
            permits ArgvAdmissionPhase, BulkAdmissionPhase, InlineInspectionPhase,
            InlineAdmissionPhase, RequestAdmissionPhase {
        PendingAdmission admission();
    }

    private record ArgvAdmissionPhase(int argc, PendingAdmission admission) implements AdmissionPhase {
        @Override
        public String stateName() {
            return "WAITING_FOR_ARGV";
        }

        @Override
        public long reservedBytes() {
            return admission.consumed ? admission.bytes : 0L;
        }
    }

    private static final class ArrayProgress {
        private final byte[][] argv;
        private int argIndex;
        private int retainedBytes;
        private long reservedBytes;

        private ArrayProgress(byte[][] argv, long reservedBytes) {
            this.argv = argv;
            this.reservedBytes = reservedBytes;
        }
    }

    private record ArrayPhase(ArrayProgress array) implements DecodePhase {
        @Override
        public String stateName() {
            return "READ_ARRAY_BODY";
        }

        @Override
        public long reservedBytes() {
            return array.reservedBytes;
        }
    }

    private record BulkAdmissionPhase(
            ArrayProgress array,
            int length,
            PendingAdmission admission
    ) implements AdmissionPhase {
        @Override
        public String stateName() {
            return "WAITING_FOR_BULK";
        }

        @Override
        public long reservedBytes() {
            return array.reservedBytes;
        }
    }

    private record BulkReadyPhase(ArrayProgress array, int length) implements DecodePhase {
        @Override
        public String stateName() {
            return "READ_ARRAY_BODY";
        }

        @Override
        public long reservedBytes() {
            return array.reservedBytes;
        }
    }

    private static final class BulkBodyPhase implements DecodePhase {
        private final ArrayProgress array;
        private final int length;
        private final byte[] buffer;
        private int bytesRead;

        private BulkBodyPhase(ArrayProgress array, int length, byte[] buffer) {
            this.array = array;
            this.length = length;
            this.buffer = buffer;
        }

        private ArrayProgress array() {
            return array;
        }

        private int length() {
            return length;
        }

        private byte[] buffer() {
            return buffer;
        }

        @Override
        public String stateName() {
            return "READ_ARRAY_BODY";
        }

        @Override
        public long reservedBytes() {
            return array.reservedBytes;
        }
    }

    private record InlineInspectionPhase(
            int length,
            PendingAdmission admission
    ) implements AdmissionPhase {
        @Override
        public String stateName() {
            return "WAITING_FOR_INLINE";
        }

        @Override
        public long reservedBytes() {
            return admission.consumed ? admission.bytes : 0L;
        }
    }

    private static final class InlineAdmissionPhase implements AdmissionPhase {
        private final InlineCommandParser.Parsed parsed;
        private final PendingAdmission admission;
        private long reservedBytes;

        private InlineAdmissionPhase(
                InlineCommandParser.Parsed parsed,
                long reservedBytes,
                PendingAdmission admission
        ) {
            this.parsed = parsed;
            this.reservedBytes = reservedBytes;
            this.admission = admission;
        }

        private InlineCommandParser.Parsed parsed() {
            return parsed;
        }

        @Override
        public PendingAdmission admission() {
            return admission;
        }

        @Override
        public String stateName() {
            return "WAITING_FOR_INLINE";
        }

        @Override
        public long reservedBytes() {
            return reservedBytes;
        }
    }

    private record RequestAdmissionPhase(
            ArrayProgress array,
            PendingAdmission admission
    ) implements AdmissionPhase {
        @Override
        public String stateName() {
            return "WAITING_FOR_HANDOFF";
        }

        @Override
        public long reservedBytes() {
            return array.reservedBytes;
        }
    }

    private static final class HandoffPhase implements DecodePhase {
        private final RespDecodedMessage message;
        private boolean admissionInProgress;

        private HandoffPhase(RespDecodedMessage message) {
            this.message = message;
        }

        private RespDecodedMessage message() {
            return message;
        }

        private boolean terminal() {
            return message instanceof RespProtocolError;
        }

        @Override
        public String stateName() {
            return terminal() ? "CLOSING" : "WAITING_FOR_HANDOFF";
        }

        @Override
        public boolean acceptsInput() {
            return !terminal();
        }
    }

    private static final class HandoffAttempt {
        private final AtomicBoolean scheduled = new AtomicBoolean();
    }

    private static final class PendingAdmission {
        private final long bytes;
        private final long inputReleaseCredit;
        private volatile boolean granted;
        private boolean consumed;

        private PendingAdmission(long bytes, long inputReleaseCredit) {
            this.bytes = bytes;
            this.inputReleaseCredit = inputReleaseCredit;
        }
    }
}

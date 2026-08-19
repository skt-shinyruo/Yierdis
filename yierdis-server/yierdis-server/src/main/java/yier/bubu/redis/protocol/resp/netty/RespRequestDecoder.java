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

    private enum State {
        READ_COMMAND,
        WAITING_FOR_ARGV,
        READ_ARRAY_BODY,
        WAITING_FOR_BULK,
        WAITING_FOR_INLINE,
        WAITING_FOR_HANDOFF,
        CLOSING
    }

    private final int maxBulkBytes;
    private final int maxArgs;
    private final int maxInlineBytes;
    private final int maxCommandBytes;
    private final InboundMemoryBudget budget;
    private final InboundConnectionMemory connection;
    private final RespDecodedMessageGate decodedMessageGate;
    private final ByteBufLineView inlineLineView = new ByteBufLineView();

    private State state = State.READ_COMMAND;
    private AccountedRespCumulator cumulator;
    private InboundReadControl readControl = InboundReadControl.NOOP;
    private byte[][] pendingArgv;
    private int pendingArgc;
    private int pendingArgIndex;
    private int pendingRetainedBytes;
    private int pendingBulkLength = -1;
    private byte[] pendingBulkBuffer;
    private int pendingBulkBytesRead;
    private long pendingReservedBytes;
    private PendingAdmission pendingAdmission;
    private boolean bulkAdmissionGranted;
    private boolean pendingRequestCredit;
    private InlineCommandParser.Parsed pendingInline;
    private long pendingInlineAdmissionBytes;
    private RespDecodedMessage pendingDecodedMessage;
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
        if (state == State.CLOSING) {
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
        return state.name();
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
        if (state == State.CLOSING || cumulator == null) {
            return;
        }
        if (!completeConsolidation(ctx)) {
            return;
        }
        if (!resumePendingState(ctx)) {
            return;
        }

        ByteBuf in = cumulator.buffer();
        while (state != State.CLOSING && in.isReadable()) {
            ParseResult result;
            if (state == State.READ_COMMAND) {
                byte first = in.getByte(in.readerIndex());
                result = first == ARRAY ? tryStartArray(ctx, in) : tryReadInline(ctx, in);
            } else if (state == State.READ_ARRAY_BODY) {
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
            if (!resumePendingState(ctx)) {
                return;
            }
        }
        cumulator.discardFullyReadComponents();
        if (state != State.CLOSING) {
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

    private boolean resumePendingState(ChannelHandlerContext ctx) {
        if (state == State.WAITING_FOR_ARGV) {
            if (!consumeProgressAdmission(ctx, outerArgvCharge(pendingArgc), cumulator.releasableChargeAfterRead(0))) {
                return false;
            }
            allocatePendingArgv();
            state = State.READ_ARRAY_BODY;
        }
        if (state == State.WAITING_FOR_BULK) {
            PendingAdmission pending = pendingAdmission;
            if (pending == null || !pending.granted) {
                return false;
            }
            state = State.READ_ARRAY_BODY;
        }
        if (state == State.WAITING_FOR_INLINE) {
            long remainingAdmission = pendingInlineAdmissionBytes - pendingReservedBytes;
            if (!consumeProgressAdmission(ctx, remainingAdmission, 0L)) {
                return false;
            }
            pendingReservedBytes = pendingInlineAdmissionBytes;
            if (finishPendingInline(ctx) != ParseResult.CONTINUE) {
                return false;
            }
        }
        if (state == State.WAITING_FOR_HANDOFF) {
            if (pendingRequestCredit) {
                if (!consumeProgressAdmission(ctx, REQUEST_FIXED_BYTES, cumulator.releasableChargeAfterRead(0))) {
                    return false;
                }
                pendingRequestCredit = false;
                pendingReservedBytes = InboundMemoryBudget.saturatedAdd(pendingReservedBytes, REQUEST_FIXED_BYTES);
                pendingDecodedMessage = buildCompletedRequest();
            }
            if (pendingDecodedMessage != null) {
                return forwardPendingDecodedMessage(ctx);
            }
        }
        return state != State.CLOSING;
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

        pendingArgc = argc;
        if (!consumeProgressAdmission(ctx, outerArgvCharge(argc), cumulator.releasableChargeAfterRead(0))) {
            state = State.WAITING_FOR_ARGV;
            return ParseResult.WAITING;
        }
        allocatePendingArgv();
        state = State.READ_ARRAY_BODY;
        return ParseResult.CONTINUE;
    }

    private void allocatePendingArgv() {
        long charge = outerArgvCharge(pendingArgc);
        pendingReservedBytes = InboundMemoryBudget.saturatedAdd(pendingReservedBytes, charge);
        pendingArgv = new byte[pendingArgc][];
        allocatedArgvArrays++;
        pendingArgIndex = 0;
        pendingRetainedBytes = 0;
        pendingBulkLength = -1;
        cumulator.discardFullyReadComponents();
    }

    private ParseResult tryContinueArray(ChannelHandlerContext ctx, ByteBuf in) {
        while (pendingArgIndex < pendingArgc) {
            if (pendingBulkLength < 0) {
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
                    pendingArgv[pendingArgIndex++] = null;
                    cumulator.discardFullyReadComponents();
                    continue;
                }
                int length = lenValue.intValue();
                if (maxBulkBytes > 0 && length > maxBulkBytes) {
                    emitProtocolError(ctx, "ERR Protocol error: invalid bulk length");
                    return ParseResult.ERROR;
                }
                pendingBulkLength = length;
            }

            if (maxCommandBytes > 0 && pendingRetainedBytes > maxCommandBytes - pendingBulkLength) {
                emitProtocolError(ctx, "ERR Protocol error: command is too large");
                return ParseResult.ERROR;
            }
            if (!admitPendingBulk(ctx)) {
                if (state != State.CLOSING) {
                    state = State.WAITING_FOR_BULK;
                    return ParseResult.WAITING;
                }
                return ParseResult.ERROR;
            }
            int remainingBody = pendingBulkLength - pendingBulkBytesRead;
            if (remainingBody > 0 && in.isReadable()) {
                int copied = Math.min(remainingBody, in.readableBytes());
                in.readBytes(pendingBulkBuffer, pendingBulkBytesRead, copied);
                pendingBulkBytesRead += copied;
                cumulator.discardFullyReadComponents();
            }
            if (pendingBulkBytesRead < pendingBulkLength) {
                return ParseResult.NEED_MORE;
            }

            if (in.readableBytes() < 2L) {
                return ParseResult.NEED_MORE;
            }

            byte[] arg = pendingBulkBuffer;
            pendingBulkBuffer = null;
            pendingBulkBytesRead = 0;
            byte cr = in.readByte();
            byte lf = in.readByte();
            if (cr != CR || lf != LF) {
                emitProtocolError(ctx, "ERR Protocol error: invalid bulk string terminator");
                return ParseResult.ERROR;
            }
            pendingArgv[pendingArgIndex++] = arg;
            pendingRetainedBytes = saturatedAdd(pendingRetainedBytes, pendingBulkLength);
            pendingBulkLength = -1;
            bulkAdmissionGranted = false;
            cumulator.discardFullyReadComponents();
        }

        return completeRequest(ctx);
    }

    private boolean admitPendingBulk(ChannelHandlerContext ctx) {
        if (bulkAdmissionGranted) {
            return true;
        }
        long charge = payloadCharge(pendingBulkLength);
        long releaseCredit = cumulator.releasableChargeAfterRead(0);
        if (!consumeProgressAdmission(ctx, charge, releaseCredit)) {
            return false;
        }
        pendingReservedBytes = InboundMemoryBudget.saturatedAdd(pendingReservedBytes, charge);
        try {
            pendingBulkBuffer = new byte[pendingBulkLength];
            allocatedBulkArrays++;
        } catch (OutOfMemoryError failure) {
            emitRequestMemoryError(ctx);
            return false;
        }
        pendingBulkBytesRead = 0;
        bulkAdmissionGranted = true;
        cumulator.discardFullyReadComponents();
        return true;
    }

    private ParseResult completeRequest(ChannelHandlerContext ctx) {
        if (!consumeProgressAdmission(ctx, REQUEST_FIXED_BYTES, cumulator.releasableChargeAfterRead(0))) {
            pendingRequestCredit = true;
            state = State.WAITING_FOR_HANDOFF;
            return ParseResult.WAITING;
        }
        pendingReservedBytes = InboundMemoryBudget.saturatedAdd(pendingReservedBytes, REQUEST_FIXED_BYTES);
        pendingDecodedMessage = buildCompletedRequest();
        state = State.WAITING_FOR_HANDOFF;
        return forwardPendingDecodedMessage(ctx) ? ParseResult.CONTINUE : ParseResult.WAITING;
    }

    private RespDecodedMessage buildCompletedRequest() {
        byte[][] argv = pendingArgv;
        int retainedBytes = pendingRetainedBytes;
        long reservedBytes = pendingReservedBytes;
        pendingArgv = null;
        pendingArgc = 0;
        pendingArgIndex = 0;
        pendingRetainedBytes = 0;
        pendingBulkLength = -1;
        pendingBulkBuffer = null;
        pendingBulkBytesRead = 0;
        pendingReservedBytes = 0L;
        RequestMemoryLease lease = budget == null
                ? new ReferenceCountedRequestMemoryLease(reservedBytes, ignored -> { })
                : requestLease(reservedBytes);
        return new RespDecodedMessage.Request(
                ByteArrayExecutionRequest.takeOwnership(argv, retainedBytes, lease)
        );
    }

    private boolean forwardPendingDecodedMessage(ChannelHandlerContext ctx) {
        RespDecodedMessage decoded = pendingDecodedMessage;
        if (decoded == null) {
            state = State.READ_COMMAND;
            readControl.resumeIngress();
            return true;
        }
        boolean terminal = switch (decoded) {
            case RespDecodedMessage.Request ignored -> false;
            case RespProtocolError ignored -> true;
        };
        pendingDecodedMessage = null;
        state = terminal ? State.CLOSING : State.READ_COMMAND;
        if (terminal) {
            readControl.pauseIngress();
        }
        RespDecodedMessageGate.Admission admission;
        try {
            admission = decodedMessageGate.tryAdmit(ctx, decoded, () -> resumeHandoffLater(ctx));
        } catch (Throwable ignored) {
            decoded.close();
            enterClosing();
            return false;
        }
        if (admission == null) {
            decoded.close();
            enterClosing();
            return false;
        }
        return switch (admission) {
            case ADMITTED -> {
                if (!terminal) {
                    readControl.resumeIngress();
                }
                yield !terminal;
            }
            case WAITING -> {
                pendingDecodedMessage = decoded;
                state = State.WAITING_FOR_HANDOFF;
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
        if (!consumeProgressAdmission(ctx, inspectionBytes, inputReleaseCredit)) {
            if (state != State.CLOSING) {
                in.readerIndex(lineStart);
                return ParseResult.WAITING;
            }
            return ParseResult.ERROR;
        }
        pendingReservedBytes = inspectionBytes;
        try {
            byte[] line = new byte[length];
            in.getBytes(lineStart, line);
            cumulator.discardFullyReadComponents();
            InlineCommandParser.Parsed parsed = InlineCommandParser.parseResult(line, 0, line.length);
            if (parsed.argc() == 0) {
                releaseInlineTransient(pendingReservedBytes, 0L);
                pendingReservedBytes = 0L;
                return ParseResult.CONTINUE;
            }
            if (maxArgs > 0 && parsed.argc() > maxArgs) {
                emitProtocolError(ctx, "ERR Protocol error: too many arguments");
                return ParseResult.ERROR;
            }
            if (maxCommandBytes > 0 && parsed.retainedBytes() > maxCommandBytes) {
                emitProtocolError(ctx, "ERR Protocol error: command is too large");
                return ParseResult.ERROR;
            }

            pendingInline = parsed;
            pendingInlineAdmissionBytes = inlineAdmissionCharge(
                    length,
                    parsed.argc(),
                    parsed.retainedBytes()
            );
            long remainingAdmission = pendingInlineAdmissionBytes - pendingReservedBytes;
            if (!consumeProgressAdmission(ctx, remainingAdmission, 0L)) {
                if (state != State.CLOSING) {
                    state = State.WAITING_FOR_INLINE;
                    return ParseResult.WAITING;
                }
                return ParseResult.ERROR;
            }
            pendingReservedBytes = pendingInlineAdmissionBytes;
            return finishPendingInline(ctx);
        } catch (IllegalArgumentException e) {
            emitProtocolError(ctx, "ERR Protocol error: invalid inline command");
            return ParseResult.ERROR;
        }
    }

    private ParseResult finishPendingInline(ChannelHandlerContext ctx) {
        InlineCommandParser.Parsed parsed = pendingInline;
        byte[][] argv = parsed.takeArgs();
        allocatedArgvArrays++;
        long fullCharge = ByteArrayExecutionRequest.estimatedMemoryBytes(argv);
        if (fullCharge > pendingReservedBytes) {
            emitRequestMemoryError(ctx);
            return ParseResult.ERROR;
        }
        releaseInlineTransient(pendingReservedBytes, fullCharge);
        pendingReservedBytes = fullCharge;
        pendingInline = null;
        pendingInlineAdmissionBytes = 0L;
        RequestMemoryLease lease = requestLease(fullCharge);
        pendingDecodedMessage = new RespDecodedMessage.Request(
                ByteArrayExecutionRequest.takeOwnership(argv, parsed.retainedBytes(), lease)
        );
        pendingReservedBytes = 0L;
        state = State.WAITING_FOR_HANDOFF;
        return forwardPendingDecodedMessage(ctx) ? ParseResult.CONTINUE : ParseResult.WAITING;
    }

    private boolean consumeProgressAdmission(ChannelHandlerContext ctx, long bytes, long inputReleaseCredit) {
        return consumePendingAdmission(ctx, bytes, inputReleaseCredit, true);
    }

    private boolean consumePendingAdmission(
            ChannelHandlerContext ctx,
            long bytes,
            long inputReleaseCredit,
            boolean progressReservation
    ) {
        if (bytes < 0L || inputReleaseCredit < 0L) {
            emitRequestMemoryError(ctx);
            return false;
        }
        PendingAdmission pending = pendingAdmission;
        if (pending != null) {
            if (!pending.matches(bytes, inputReleaseCredit, progressReservation) || !pending.granted) {
                return false;
            }
            pendingAdmission = null;
            return true;
        }
        if (budget == null) {
            return true;
        }
        PendingAdmission requested = new PendingAdmission(bytes, inputReleaseCredit, progressReservation);
        pendingAdmission = requested;
        connection.setResumeCallback(ctx.executor(), () -> {
            if (pendingAdmission == requested && connection.claimGrantedReservation(requested.bytes)) {
                requested.granted = true;
                resumeOnEventLoop(ctx);
            }
        });
        InboundMemoryBudget.ReservationResult result = progressReservation
                ? budget.tryTransferForProgress(connection, bytes, inputReleaseCredit)
                : budget.tryTransfer(connection, bytes, inputReleaseCredit);
        if (result == InboundMemoryBudget.ReservationResult.RESERVED) {
            pendingAdmission = null;
            return true;
        }
        if (result == InboundMemoryBudget.ReservationResult.WAITING) {
            readControl.pauseIngress();
            return false;
        }
        pendingAdmission = null;
        emitRequestMemoryError(ctx);
        return false;
    }

    private void resumeOnEventLoop(ChannelHandlerContext ctx) {
        if (ctx.executor().inEventLoop()) {
            process(ctx);
            return;
        }
        ctx.executor().execute(() -> process(ctx));
    }

    private void resumeHandoffLater(ChannelHandlerContext ctx) {
        try {
            // WAITING 返回后 decoder 才恢复 pending 消息状态，因此 handoff wake-up 不得同步重入。
            ctx.executor().execute(() -> process(ctx));
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
        resetPendingState();
        enterClosing();
        if (cumulator != null) {
            cumulator.close();
        }
        pendingDecodedMessage = new RespProtocolError(message);
        forwardPendingDecodedMessage(ctx);
    }

    private void enterClosing() {
        state = State.CLOSING;
        readControl.pauseIngress();
    }

    private void cleanup() {
        resetPendingState();
        if (cumulator != null) {
            cumulator.close();
            cumulator = null;
        }
        readControl.pauseIngress();
    }

    private void resetPendingState() {
        releasePendingAdmission();
        if (pendingDecodedMessage != null) {
            pendingDecodedMessage.close();
            pendingDecodedMessage = null;
        }
        if (pendingReservedBytes > 0L && budget != null) {
            budget.release(connection.account(), pendingReservedBytes);
        }
        pendingArgv = null;
        pendingArgc = 0;
        pendingArgIndex = 0;
        pendingRetainedBytes = 0;
        pendingBulkLength = -1;
        pendingBulkBuffer = null;
        pendingBulkBytesRead = 0;
        pendingReservedBytes = 0L;
        pendingRequestCredit = false;
        bulkAdmissionGranted = false;
        pendingInline = null;
        pendingInlineAdmissionBytes = 0L;
    }

    private void releasePendingAdmission() {
        PendingAdmission pending = pendingAdmission;
        pendingAdmission = null;
        if (pending == null || budget == null || connection == null) {
            return;
        }
        if (pending.granted || connection.claimGrantedReservation(pending.bytes)) {
            budget.release(connection, pending.bytes);
        } else {
            budget.cancelWaiter(connection);
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

    private static final class PendingAdmission {
        private final long bytes;
        private final long inputReleaseCredit;
        private final boolean progressReservation;
        private volatile boolean granted;

        private PendingAdmission(long bytes, long inputReleaseCredit, boolean progressReservation) {
            this.bytes = bytes;
            this.inputReleaseCredit = inputReleaseCredit;
            this.progressReservation = progressReservation;
        }

        private boolean matches(
                long candidateBytes,
                long candidateInputReleaseCredit,
                boolean candidateProgressReservation
        ) {
            return bytes == candidateBytes
                    && inputReleaseCredit == candidateInputReleaseCredit
                    && progressReservation == candidateProgressReservation;
        }
    }
}

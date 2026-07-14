package yier.bubu.redis.protocol.resp.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import yier.bubu.redis.execution.api.ExecutionRequest;
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
    private static final int INLINE_PARSER_INITIAL_METADATA_CAPACITY = 16;
    private static final long INLINE_DECODED_OBJECT_BYTES = 32L;
    private static final long INLINE_PARSER_TRANSIENT_FLOOR_BYTES = 512L;
    private static final int MAX_COMPONENTS = 16;
    private static final int INCOMPLETE_LINE = Integer.MIN_VALUE;
    private static final int INVALID_LINE = -1;
    private static final long INVALID_INLINE_SHAPE = -1L;

    private enum State {
        READ_COMMAND,
        WAITING_FOR_ARGV,
        READ_ARRAY_BODY,
        WAITING_FOR_BULK,
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

    private State state = State.READ_COMMAND;
    private AccountedRespCumulator cumulator;
    private InboundReadControl readControl = InboundReadControl.NOOP;
    private byte[][] pendingArgv;
    private int pendingArgc;
    private int pendingArgIndex;
    private int pendingRetainedBytes;
    private int pendingBulkLength = -1;
    private long pendingReservedBytes;
    private PendingAdmission pendingAdmission;
    private boolean bulkAdmissionGranted;
    private boolean pendingRequestCredit;
    private Object pendingDecodedMessage;
    private int allocatedArgvArrays;
    private int allocatedBulkArrays;

    public RespRequestDecoder(int maxBulkBytes, int maxArgs, int maxInlineBytes, int maxCommandBytes) {
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
                readControl.resumeIngress();
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
            if (!consumePendingAdmission(ctx, outerArgvCharge(pendingArgc), cumulator.releasableChargeAfterRead(0))) {
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
        if (state == State.WAITING_FOR_HANDOFF) {
            if (pendingRequestCredit) {
                if (!consumePendingAdmission(ctx, REQUEST_FIXED_BYTES, cumulator.releasableChargeAfterRead(0))) {
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
        if (argcValue == null || argcValue < 0 || argcValue > RespProtocolLimits.MAX_ARGS) {
            emitProtocolError(ctx, "ERR Protocol error: invalid multibulk length");
            return ParseResult.ERROR;
        }
        int argc = argcValue.intValue();
        if (maxArgs > 0 && argc > maxArgs) {
            emitProtocolError(ctx, "ERR Protocol error: too many arguments");
            return ParseResult.ERROR;
        }

        pendingArgc = argc;
        if (!consumePendingAdmission(ctx, outerArgvCharge(argc), cumulator.releasableChargeAfterRead(0))) {
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
                if (lenValue == null || lenValue < -1 || lenValue > RespProtocolLimits.MAX_BULK_BYTES) {
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
            if (in.readableBytes() < pendingBulkLength + 2L) {
                return ParseResult.NEED_MORE;
            }

            byte[] arg = new byte[pendingBulkLength];
            allocatedBulkArrays++;
            in.readBytes(arg);
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
        if (!consumePendingAdmission(ctx, charge, releaseCredit)) {
            return false;
        }
        pendingReservedBytes = InboundMemoryBudget.saturatedAdd(pendingReservedBytes, charge);
        bulkAdmissionGranted = true;
        cumulator.discardFullyReadComponents();
        return true;
    }

    private ParseResult completeRequest(ChannelHandlerContext ctx) {
        if (!consumePendingAdmission(ctx, REQUEST_FIXED_BYTES, cumulator.releasableChargeAfterRead(0))) {
            pendingRequestCredit = true;
            state = State.WAITING_FOR_HANDOFF;
            return ParseResult.WAITING;
        }
        pendingReservedBytes = InboundMemoryBudget.saturatedAdd(pendingReservedBytes, REQUEST_FIXED_BYTES);
        pendingDecodedMessage = buildCompletedRequest();
        state = State.WAITING_FOR_HANDOFF;
        return forwardPendingDecodedMessage(ctx) ? ParseResult.CONTINUE : ParseResult.WAITING;
    }

    private Object buildCompletedRequest() {
        byte[][] argv = pendingArgv;
        int retainedBytes = pendingRetainedBytes;
        long reservedBytes = pendingReservedBytes;
        pendingArgv = null;
        pendingArgc = 0;
        pendingArgIndex = 0;
        pendingRetainedBytes = 0;
        pendingBulkLength = -1;
        pendingReservedBytes = 0L;
        RequestMemoryLease lease = budget == null
                ? new ReferenceCountedRequestMemoryLease(reservedBytes, ignored -> { })
                : requestLease(reservedBytes);
        return RetainedRespExecutionRequest.takeOwnership(argv, retainedBytes, lease);
    }

    private boolean forwardPendingDecodedMessage(ChannelHandlerContext ctx) {
        Object decoded = pendingDecodedMessage;
        if (decoded == null) {
            state = State.READ_COMMAND;
            readControl.resumeIngress();
            return true;
        }
        RespDecodedMessageGate.Admission admission;
        try {
            admission = decodedMessageGate.tryAdmit(ctx, decoded, () -> resumeOnEventLoop(ctx));
        } catch (Throwable ignored) {
            closeDecoded(decoded);
            pendingDecodedMessage = null;
            enterClosing();
            return false;
        }
        if (admission == null || admission.status() == RespDecodedMessageGate.Status.CLOSED) {
            closeDecoded(decoded);
            pendingDecodedMessage = null;
            enterClosing();
            return false;
        }
        if (admission.status() == RespDecodedMessageGate.Status.WAITING) {
            state = State.WAITING_FOR_HANDOFF;
            readControl.pauseIngress();
            return false;
        }
        pendingDecodedMessage = null;
        if (decoded instanceof RespProtocolError) {
            state = State.CLOSING;
            readControl.pauseIngress();
            ctx.fireChannelRead(admission.forwardedMessage());
            return false;
        }
        state = State.READ_COMMAND;
        readControl.resumeIngress();
        ctx.fireChannelRead(admission.forwardedMessage());
        return true;
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
        if (lineEnd <= lineStart || isBlank(in, lineStart, lineEnd)) {
            cumulator.discardFullyReadComponents();
            return ParseResult.CONTINUE;
        }

        int length = lineEnd - lineStart;
        if (maxCommandBytes > 0 && length > maxCommandBytes) {
            emitProtocolError(ctx, "ERR Protocol error: command is too large");
            return ParseResult.ERROR;
        }

        long shape = scanInlineShape(in, lineStart, lineEnd);
        if (shape == INVALID_INLINE_SHAPE) {
            emitProtocolError(ctx, "ERR Protocol error: invalid inline command");
            return ParseResult.ERROR;
        }
        int argc = inlineArgc(shape);
        int retainedBytes = inlineRetainedBytes(shape);
        if (maxArgs > 0 && argc > maxArgs) {
            emitProtocolError(ctx, "ERR Protocol error: too many arguments");
            return ParseResult.ERROR;
        }
        if (maxCommandBytes > 0 && retainedBytes > maxCommandBytes) {
            emitProtocolError(ctx, "ERR Protocol error: command is too large");
            return ParseResult.ERROR;
        }

        long admissionBytes = inlineAdmissionCharge(length, argc, retainedBytes);
        long inputReleaseCredit = cumulator.releasableChargeAfterRead(0);
        if (!consumePendingAdmission(ctx, admissionBytes, inputReleaseCredit)) {
            if (state != State.CLOSING) {
                in.readerIndex(lineStart);
                return ParseResult.WAITING;
            }
            return ParseResult.ERROR;
        }
        pendingReservedBytes = admissionBytes;
        try {
            byte[][] argv = parseInline(in, lineStart, length);
            long fullCharge = RetainedRespExecutionRequest.estimatedMemoryBytes(argv);
            if (fullCharge > admissionBytes) {
                emitRequestMemoryError(ctx);
                return ParseResult.ERROR;
            }
            cumulator.discardFullyReadComponents();
            releaseInlineTransient(admissionBytes, fullCharge);
            pendingReservedBytes = fullCharge;
            RequestMemoryLease lease = requestLease(fullCharge);
            pendingDecodedMessage = RetainedRespExecutionRequest.takeOwnership(argv, retainedBytes, lease);
            pendingReservedBytes = 0L;
            state = State.WAITING_FOR_HANDOFF;
            return forwardPendingDecodedMessage(ctx) ? ParseResult.CONTINUE : ParseResult.WAITING;
        } catch (IllegalArgumentException e) {
            emitProtocolError(ctx, "ERR Protocol error: invalid inline command");
            return ParseResult.ERROR;
        }
    }

    private boolean consumePendingAdmission(ChannelHandlerContext ctx, long bytes, long inputReleaseCredit) {
        if (bytes < 0L || inputReleaseCredit < 0L) {
            emitRequestMemoryError(ctx);
            return false;
        }
        PendingAdmission pending = pendingAdmission;
        if (pending != null) {
            if (!pending.matches(bytes, inputReleaseCredit) || !pending.granted) {
                return false;
            }
            pendingAdmission = null;
            return true;
        }
        if (budget == null) {
            return true;
        }
        PendingAdmission requested = new PendingAdmission(bytes, inputReleaseCredit);
        pendingAdmission = requested;
        connection.setResumeCallback(ctx.executor(), () -> {
            if (pendingAdmission == requested && connection.claimGrantedReservation(requested.bytes)) {
                requested.granted = true;
                resumeOnEventLoop(ctx);
            }
        });
        InboundMemoryBudget.ReservationResult result = budget.tryTransfer(connection, bytes, inputReleaseCredit);
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
        pendingDecodedMessage = new RespProtocolError(message, true);
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
            closeDecoded(pendingDecodedMessage);
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
        pendingReservedBytes = 0L;
        pendingRequestCredit = false;
        bulkAdmissionGranted = false;
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

    private static void closeDecoded(Object decoded) {
        if (decoded instanceof ExecutionRequest request) {
            request.close();
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

    private static byte[][] parseInline(ByteBuf in, int lineStart, int length) {
        byte[] line = new byte[length];
        in.getBytes(lineStart, line);
        InlineCommandParser.Decoded decoded = InlineCommandParser.parseUnlimited(line, 0, line.length);
        return decoded.copyArgs();
    }

    private static long inlineAdmissionCharge(int lineLength, int argc, int retainedBytes) {
        long finalRequestCharge = inlineFinalChargeUpperBound(argc, retainedBytes);
        long parserTransient = InboundMemoryBudget.saturatedAdd(payloadCharge(lineLength), payloadCharge(lineLength));
        parserTransient = InboundMemoryBudget.saturatedAdd(parserTransient, inlineParserMetadataCharge(argc));
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

    private static long inlineParserMetadataCharge(int argc) {
        int capacity = INLINE_PARSER_INITIAL_METADATA_CAPACITY;
        long total = 0L;
        while (true) {
            long oneArray = primitiveArrayCharge(capacity, Integer.BYTES);
            total = InboundMemoryBudget.saturatedAdd(total, oneArray);
            total = InboundMemoryBudget.saturatedAdd(total, oneArray);
            if (capacity >= argc) {
                return total;
            }
            if (capacity > Integer.MAX_VALUE / 2) {
                return Long.MAX_VALUE;
            }
            capacity <<= 1;
        }
    }

    private static long primitiveArrayCharge(int length, int elementBytes) {
        long payload = saturatedMultiply(Math.max(0, length), Math.max(0, elementBytes));
        return InboundMemoryBudget.saturatedAdd(ARRAY_HEADER_BYTES, align8(payload));
    }

    private static long scanInlineShape(ByteBuf in, int start, int end) {
        int position = start;
        int argc = 0;
        int retainedBytes = 0;
        while (true) {
            while (position < end && isInlineSpace(in.getByte(position))) {
                position++;
            }
            if (position >= end) {
                break;
            }

            boolean doubleQuoted = false;
            boolean singleQuoted = false;
            int argumentLength = 0;
            while (position < end) {
                byte value = in.getByte(position);
                if (doubleQuoted) {
                    if (value == '\\'
                            && position + 3 < end
                            && in.getByte(position + 1) == 'x'
                            && isHexDigit(in.getByte(position + 2))
                            && isHexDigit(in.getByte(position + 3))) {
                        argumentLength++;
                        position += 4;
                        continue;
                    }
                    if (value == '\\' && position + 1 < end) {
                        argumentLength++;
                        position += 2;
                        continue;
                    }
                    if (value == '"') {
                        if (position + 1 < end && !isInlineSpace(in.getByte(position + 1))) {
                            return INVALID_INLINE_SHAPE;
                        }
                        doubleQuoted = false;
                        position++;
                        break;
                    }
                    argumentLength++;
                    position++;
                    continue;
                }

                if (singleQuoted) {
                    if (value == '\\' && position + 1 < end && in.getByte(position + 1) == '\'') {
                        argumentLength++;
                        position += 2;
                        continue;
                    }
                    if (value == '\'') {
                        if (position + 1 < end && !isInlineSpace(in.getByte(position + 1))) {
                            return INVALID_INLINE_SHAPE;
                        }
                        singleQuoted = false;
                        position++;
                        break;
                    }
                    argumentLength++;
                    position++;
                    continue;
                }

                if (isInlineSpace(value)) {
                    break;
                }
                if (value == '"') {
                    doubleQuoted = true;
                    position++;
                    continue;
                }
                if (value == '\'') {
                    singleQuoted = true;
                    position++;
                    continue;
                }
                argumentLength++;
                position++;
            }
            if (doubleQuoted || singleQuoted || argc == Integer.MAX_VALUE) {
                return INVALID_INLINE_SHAPE;
            }
            retainedBytes = saturatedAdd(retainedBytes, argumentLength);
            argc++;
        }
        if (argc == 0) {
            return INVALID_INLINE_SHAPE;
        }
        return ((long) argc << Integer.SIZE) | (retainedBytes & 0xFFFF_FFFFL);
    }

    private static int inlineArgc(long shape) {
        return (int) (shape >>> Integer.SIZE);
    }

    private static int inlineRetainedBytes(long shape) {
        return (int) shape;
    }

    private static boolean isInlineSpace(byte value) {
        return value == ' ' || value == '\t';
    }

    private static boolean isHexDigit(byte value) {
        return (value >= '0' && value <= '9')
                || (value >= 'a' && value <= 'f')
                || (value >= 'A' && value <= 'F');
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

    private static boolean isBlank(ByteBuf in, int start, int endExclusive) {
        for (int i = start; i < endExclusive; i++) {
            byte value = in.getByte(i);
            if (value != ' ' && value != '\t' && value != '\r' && value != '\n') {
                return false;
            }
        }
        return true;
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
        private volatile boolean granted;

        private PendingAdmission(long bytes, long inputReleaseCredit) {
            this.bytes = bytes;
            this.inputReleaseCredit = inputReleaseCredit;
        }

        private boolean matches(long candidateBytes, long candidateInputReleaseCredit) {
            return bytes == candidateBytes && inputReleaseCredit == candidateInputReleaseCredit;
        }
    }
}

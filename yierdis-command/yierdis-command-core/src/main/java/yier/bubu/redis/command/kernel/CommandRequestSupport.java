package yier.bubu.redis.command.kernel;

import yier.bubu.redis.execution.api.ExecutionRequest;

import java.nio.charset.StandardCharsets;

final class CommandRequestSupport {
    private CommandRequestSupport() {
    }

    static String unknownCommandMessage(ExecutionRequest request) {
        if (request == null || request.argc() <= 0 || request.isNull(0) || request.len(0) <= 0) {
            return "ERR unknown command";
        }
        int len = request.len(0);
        int printable = 0;
        for (int i = 0; i < len; i++) {
            int b = request.byteAt(0, i) & 0xFF;
            if (b >= 0x20 && b <= 0x7E && b != '\'' && b != '\\') {
                printable++;
            }
        }
        if (printable == len && len <= 64) {
            byte[] name = request.readOnlyByteArray(0);
            String s = name == null ? "" : new String(name, StandardCharsets.US_ASCII);
            return "ERR unknown command '" + s + "'";
        }
        return "ERR unknown command";
    }

    static boolean asciiEqualsIgnoreCase(ExecutionRequest request, int argIndex, String literal) {
        if (literal == null) {
            return false;
        }
        if (request.isNull(argIndex)) {
            return false;
        }
        int len = request.len(argIndex);
        if (len != literal.length()) {
            return false;
        }
        for (int i = 0; i < len; i++) {
            int b = request.byteAt(argIndex, i) & 0xFF;
            int c = literal.charAt(i);
            if (b >= 'A' && b <= 'Z') {
                b |= 0x20;
            }
            if (c >= 'A' && c <= 'Z') {
                c |= 0x20;
            }
            if (b != c) {
                return false;
            }
        }
        return true;
    }
}

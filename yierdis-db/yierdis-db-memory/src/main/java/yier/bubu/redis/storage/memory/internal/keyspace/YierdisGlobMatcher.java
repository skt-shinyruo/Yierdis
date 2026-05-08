package yier.bubu.redis.storage.memory.internal.keyspace;

import yier.bubu.redis.storage.memory.*;
import yier.bubu.redis.storage.memory.internal.expire.*;
import yier.bubu.redis.storage.memory.internal.ffm.*;
import yier.bubu.redis.storage.memory.internal.key.*;
import yier.bubu.redis.storage.memory.internal.keyspace.*;
import yier.bubu.redis.storage.memory.internal.ledger.*;
import yier.bubu.redis.storage.memory.internal.value.*;

import yier.bubu.redis.bytes.BytesView;

public final class YierdisGlobMatcher {
    private YierdisGlobMatcher() {
    }

    public static boolean matches(byte[] pattern, byte[] text) {
        if (pattern == null || text == null) {
            return false;
        }

        int p = 0;
        int t = 0;
        int star = -1;
        int starText = 0;

        while (t < text.length) {
            if (p < pattern.length) {
                byte pc = pattern[p];

                if (pc == '*') {
                    star = p++;
                    starText = t;
                    continue;
                }

                if (pc == '?') {
                    p++;
                    t++;
                    continue;
                }

                if (pc == '\\') {
                    if (p + 1 < pattern.length) {
                        byte literal = pattern[p + 1];
                        if (literal == text[t]) {
                            p += 2;
                            t++;
                            continue;
                        }
                    } else {
                        // Trailing "\" is treated as a literal backslash.
                        if (text[t] == '\\') {
                            p++;
                            t++;
                            continue;
                        }
                    }
                } else if (pc == '[') {
                    int end = findGlobClassEnd(pattern, p + 1);
                    if (end >= 0) {
                        if (globClassMatches(pattern, p + 1, end, text[t])) {
                            p = end + 1;
                            t++;
                            continue;
                        }
                    } else {
                        // Unclosed "[]" is treated as a literal '['.
                        if (text[t] == '[') {
                            p++;
                            t++;
                            continue;
                        }
                    }
                } else if (pc == text[t]) {
                    p++;
                    t++;
                    continue;
                }
            }

            if (star >= 0) {
                // Backtrack: let '*' absorb one more byte.
                p = star + 1;
                t = ++starText;
                continue;
            }
            return false;
        }

        // Remaining pattern must be empty or only "*" wildcards.
        while (p < pattern.length && pattern[p] == '*') {
            p++;
        }
        return p == pattern.length;
    }

    public static boolean matches(byte[] pattern, BytesView text) {
        if (pattern == null || text == null) {
            return false;
        }
        int textLen = text.len();
        if (textLen < 0) {
            return false;
        }

        int p = 0;
        int t = 0;
        int star = -1;
        int starText = 0;

        while (t < textLen) {
            byte tb = text.byteAt(t);
            if (p < pattern.length) {
                byte pc = pattern[p];

                if (pc == '*') {
                    star = p++;
                    starText = t;
                    continue;
                }

                if (pc == '?') {
                    p++;
                    t++;
                    continue;
                }

                if (pc == '\\') {
                    if (p + 1 < pattern.length) {
                        byte literal = pattern[p + 1];
                        if (literal == tb) {
                            p += 2;
                            t++;
                            continue;
                        }
                    } else {
                        // Trailing "\" is treated as a literal backslash.
                        if (tb == '\\') {
                            p++;
                            t++;
                            continue;
                        }
                    }
                } else if (pc == '[') {
                    int end = findGlobClassEnd(pattern, p + 1);
                    if (end >= 0) {
                        if (globClassMatches(pattern, p + 1, end, tb)) {
                            p = end + 1;
                            t++;
                            continue;
                        }
                    } else {
                        // Unclosed "[]" is treated as a literal '['.
                        if (tb == '[') {
                            p++;
                            t++;
                            continue;
                        }
                    }
                } else if (pc == tb) {
                    p++;
                    t++;
                    continue;
                }
            }

            if (star >= 0) {
                // Backtrack: let '*' absorb one more byte.
                p = star + 1;
                t = ++starText;
                continue;
            }
            return false;
        }

        // Remaining pattern must be empty or only "*" wildcards.
        while (p < pattern.length && pattern[p] == '*') {
            p++;
        }
        return p == pattern.length;
    }

    private static int findGlobClassEnd(byte[] pattern, int start) {
        if (pattern == null) {
            return -1;
        }
        int len = pattern.length;
        if (start >= len) {
            return -1;
        }

        int i = start;
        // Optional negation marker.
        if (i < len && (pattern[i] == '^' || pattern[i] == '!')) {
            i++;
        }

        boolean first = true;
        while (i < len) {
            byte c = pattern[i];
            if (c == '\\') {
                // Escaped byte inside the class.
                i += i + 1 < len ? 2 : 1;
                first = false;
                continue;
            }
            if (c == ']' && !first) {
                return i;
            }
            i++;
            first = false;
        }
        return -1;
    }

    private static boolean globClassMatches(byte[] pattern, int start, int end, byte target) {
        if (pattern == null) {
            return false;
        }
        if (start < 0 || end < start || end >= pattern.length) {
            return false;
        }

        int i = start;
        boolean negate = false;
        if (i < end && (pattern[i] == '^' || pattern[i] == '!')) {
            negate = true;
            i++;
        }

        int tb = target & 0xff;
        boolean matched = false;

        // ']' can be included as a literal if it's the first char (after optional negation).
        if (i < end && pattern[i] == ']') {
            if (tb == (']' & 0xff)) {
                matched = true;
            }
            i++;
        }

        while (i < end) {
            int c1;
            if (pattern[i] == '\\' && i + 1 < end) {
                c1 = pattern[i + 1] & 0xff;
                i += 2;
            } else {
                c1 = pattern[i] & 0xff;
                i++;
            }

            // Range: "a-z" (only if '-' is not the last char in the class)
            if (i < end - 1 && pattern[i] == '-') {
                int j = i + 1;
                int c2;
                if (pattern[j] == '\\' && j + 1 < end) {
                    c2 = pattern[j + 1] & 0xff;
                    j += 2;
                } else {
                    c2 = pattern[j] & 0xff;
                    j++;
                }

                int lo = Math.min(c1, c2);
                int hi = Math.max(c1, c2);
                if (tb >= lo && tb <= hi) {
                    matched = true;
                }
                i = j;
                continue;
            }

            if (tb == c1) {
                matched = true;
            }
        }

        return negate ? !matched : matched;
    }
}

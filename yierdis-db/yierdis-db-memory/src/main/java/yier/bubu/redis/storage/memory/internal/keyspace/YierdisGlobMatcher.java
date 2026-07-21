package yier.bubu.redis.storage.memory.internal.keyspace;

import yier.bubu.redis.bytes.BytesView;

public final class YierdisGlobMatcher {
    private YierdisGlobMatcher() {
    }

    public static boolean matches(byte[] pattern, byte[] text) {
        if (pattern == null || text == null) {
            return false;
        }
        return matchesInternal(pattern, text, null, text.length);
    }

    public static boolean matches(byte[] pattern, BytesView text) {
        if (pattern == null || text == null) {
            return false;
        }
        int textLen = text.length();
        if (textLen < 0) {
            return false;
        }

        return matchesInternal(pattern, null, text, textLen);
    }

    private static boolean matchesInternal(
            byte[] pattern,
            byte[] arrayText,
            BytesView viewText,
            int textLen
    ) {
        // 两个公开入口只提供一种 backing，避免为 byte[] 热路径创建 BytesView 适配器。
        boolean arrayBacked = arrayText != null;

        int p = 0;
        int t = 0;
        int star = -1;
        int starText = 0;

        while (t < textLen) {
            byte tb = arrayBacked ? arrayText[t] : viewText.getByte(t);
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
                        // 末尾反斜杠按普通字节匹配，这是现有 glob 兼容语义。
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
                        // 未闭合的字符类按普通 '[' 匹配，而不是直接判定 pattern 非法。
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
                // 当前分支不匹配时，让最近的 '*' 多吞一个字节后重试。
                p = star + 1;
                t = ++starText;
                continue;
            }
            return false;
        }

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
        if (i < len && (pattern[i] == '^' || pattern[i] == '!')) {
            i++;
        }

        boolean first = true;
        while (i < len) {
            byte c = pattern[i];
            if (c == '\\') {
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

        // 可选取反标记之后的首个 ']' 表示普通字符，而不是字符类结束符。
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

            // '-' 只有在后面仍有范围终点时才表示区间；末尾 '-' 按普通字符处理。
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

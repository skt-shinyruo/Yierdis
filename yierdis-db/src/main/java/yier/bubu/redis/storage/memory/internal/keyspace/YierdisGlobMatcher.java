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

        // 空文本在进入 '*' 分支之前返回：只有空模式，或恰好一个 '*'，才能匹配。
        // "**" 不是这一种，所以不能匹配空成员。
        if (textLen == 0) {
            return pattern.length == 0 || (pattern.length == 1 && pattern[0] == '*');
        }

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
                    int next = matchCharacterClass(pattern, p, tb);
                    if (next >= 0) {
                        p = next;
                        t++;
                        continue;
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

    /**
     * 对齐 Redis stringmatchlen 的字符类。取反只认 {@code ^}，{@code !} 是字面字符。
     * 紧跟 {@code [} 或 {@code [^} 的 {@code ]} 直接结束字符类，因此 {@code []a]} 和 {@code []]} 是空类。
     * 未闭合时剩余模式都是类成员。范围终点可以是 {@code ]}，例如 {@code [a-]} 覆盖 {@code ]} 到 {@code a}。
     *
     * @return 匹配成功时返回类消费之后的模式下标：闭合时在 ']' 之后，未闭合时在模式末尾。否则返回 -1。
     */
    private static int matchCharacterClass(byte[] pattern, int openBracket, byte target) {
        int index = openBracket + 1;
        int length = pattern.length;
        boolean negate = index < length && pattern[index] == '^';
        if (negate) {
            index++;
        }

        int wanted = target & 0xff;
        boolean matched = false;
        while (true) {
            if (index + 1 < length && pattern[index] == '\\') {
                index++;
                if ((pattern[index] & 0xff) == wanted) {
                    matched = true;
                }
            } else if (index >= length) {
                index--;
                break;
            } else if (pattern[index] == ']') {
                break;
            } else if (index + 2 < length && pattern[index + 1] == '-') {
                int start = pattern[index] & 0xff;
                int end = pattern[index + 2] & 0xff;
                if (start > end) {
                    int swap = start;
                    start = end;
                    end = swap;
                }
                index += 2;
                if (wanted >= start && wanted <= end) {
                    matched = true;
                }
            } else if ((pattern[index] & 0xff) == wanted) {
                matched = true;
            }
            index++;
        }
        if (negate) {
            matched = !matched;
        }
        if (!matched) {
            return -1;
        }
        return index + 1;
    }
}

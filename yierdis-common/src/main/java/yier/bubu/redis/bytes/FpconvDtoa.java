package yier.bubu.redis.bytes;

/**
 * Redis 8.0.2 {@code d2string} / {@code fpconv_dtoa}（Grisu2）的分数文本。
 * 十进制位数不是 Java {@code Double.toString} 的最短形式，但 {@code strtod} 能读回同一个值。
 *
 * <p>算法与 10 的幂表来自 Redis {@code deps/fpconv}（Boost Software License 1.0）：
 * Copyright (c) 2021, Redis Labs;
 * Copyright (c) 2013-2019, night-shift;
 * Copyright (c) 2009, Florian Loitsch.
 * 本文件是该实现的衍生作品，须保留上述版权与许可证全文，见源文件末尾。
 */
public final class FpconvDtoa {
    private static final long FRAC_MASK = 0x000FFFFFFFFFFFFFL;
    private static final long EXP_MASK = 0x7FF0000000000000L;
    private static final long HIDDEN_BIT = 0x0010000000000000L;
    private static final long SIGN_MASK = 0x8000000000000000L;
    private static final int EXP_BIAS = 1023 + 52;
    private static final int N_POWERS = 87;
    private static final int STEP_POWERS = 8;
    private static final int FIRST_POWER = -348;
    private static final int EXP_MAX = -32;
    private static final int EXP_MIN = -60;

    private static final long[] TENS = {
            Long.parseUnsignedLong("10000000000000000000"),
            Long.parseUnsignedLong("1000000000000000000"),
            Long.parseUnsignedLong("100000000000000000"),
            Long.parseUnsignedLong("10000000000000000"),
            Long.parseUnsignedLong("1000000000000000"),
            Long.parseUnsignedLong("100000000000000"),
            Long.parseUnsignedLong("10000000000000"),
            Long.parseUnsignedLong("1000000000000"),
            Long.parseUnsignedLong("100000000000"),
            10000000000L,
            1000000000L,
            100000000L,
            10000000L,
            1000000L,
            100000L,
            10000L,
            1000L,
            100L,
            10L,
            1L
    };

    private static final long[][] POWERS_TEN = {
            { Long.parseUnsignedLong("18054884314459144840"), -1220 },
            { Long.parseUnsignedLong("13451937075301367670"), -1193 },
            { Long.parseUnsignedLong("10022474136428063862"), -1166 },
            { Long.parseUnsignedLong("14934650266808366570"), -1140 },
            { Long.parseUnsignedLong("11127181549972568877"), -1113 },
            { Long.parseUnsignedLong("16580792590934885855"), -1087 },
            { Long.parseUnsignedLong("12353653155963782858"), -1060 },
            { Long.parseUnsignedLong("18408377700990114895"), -1034 },
            { Long.parseUnsignedLong("13715310171984221708"), -1007 },
            { Long.parseUnsignedLong("10218702384817765436"), -980 },
            { Long.parseUnsignedLong("15227053142812498563"), -954 },
            { Long.parseUnsignedLong("11345038669416679861"), -927 },
            { Long.parseUnsignedLong("16905424996341287883"), -901 },
            { Long.parseUnsignedLong("12595523146049147757"), -874 },
            { Long.parseUnsignedLong("9384396036005875287"), -847 },
            { Long.parseUnsignedLong("13983839803942852151"), -821 },
            { Long.parseUnsignedLong("10418772551374772303"), -794 },
            { Long.parseUnsignedLong("15525180923007089351"), -768 },
            { Long.parseUnsignedLong("11567161174868858868"), -741 },
            { Long.parseUnsignedLong("17236413322193710309"), -715 },
            { Long.parseUnsignedLong("12842128665889583758"), -688 },
            { Long.parseUnsignedLong("9568131466127621947"), -661 },
            { Long.parseUnsignedLong("14257626930069360058"), -635 },
            { Long.parseUnsignedLong("10622759856335341974"), -608 },
            { Long.parseUnsignedLong("15829145694278690180"), -582 },
            { Long.parseUnsignedLong("11793632577567316726"), -555 },
            { Long.parseUnsignedLong("17573882009934360870"), -529 },
            { Long.parseUnsignedLong("13093562431584567480"), -502 },
            { Long.parseUnsignedLong("9755464219737475723"), -475 },
            { Long.parseUnsignedLong("14536774485912137811"), -449 },
            { Long.parseUnsignedLong("10830740992659433045"), -422 },
            { Long.parseUnsignedLong("16139061738043178685"), -396 },
            { Long.parseUnsignedLong("12024538023802026127"), -369 },
            { Long.parseUnsignedLong("17917957937422433684"), -343 },
            { Long.parseUnsignedLong("13349918974505688015"), -316 },
            { Long.parseUnsignedLong("9946464728195732843"), -289 },
            { Long.parseUnsignedLong("14821387422376473014"), -263 },
            { Long.parseUnsignedLong("11042794154864902060"), -236 },
            { Long.parseUnsignedLong("16455045573212060422"), -210 },
            { Long.parseUnsignedLong("12259964326927110867"), -183 },
            { Long.parseUnsignedLong("18268770466636286478"), -157 },
            { Long.parseUnsignedLong("13611294676837538539"), -130 },
            { Long.parseUnsignedLong("10141204801825835212"), -103 },
            { Long.parseUnsignedLong("15111572745182864684"), -77 },
            { Long.parseUnsignedLong("11258999068426240000"), -50 },
            { Long.parseUnsignedLong("16777216000000000000"), -24 },
            { Long.parseUnsignedLong("12500000000000000000"), 3 },
            { Long.parseUnsignedLong("9313225746154785156"), 30 },
            { Long.parseUnsignedLong("13877787807814456755"), 56 },
            { Long.parseUnsignedLong("10339757656912845936"), 83 },
            { Long.parseUnsignedLong("15407439555097886824"), 109 },
            { Long.parseUnsignedLong("11479437019748901445"), 136 },
            { Long.parseUnsignedLong("17105694144590052135"), 162 },
            { Long.parseUnsignedLong("12744735289059618216"), 189 },
            { Long.parseUnsignedLong("9495567745759798747"), 216 },
            { Long.parseUnsignedLong("14149498560666738074"), 242 },
            { Long.parseUnsignedLong("10542197943230523224"), 269 },
            { Long.parseUnsignedLong("15709099088952724970"), 295 },
            { Long.parseUnsignedLong("11704190886730495818"), 322 },
            { Long.parseUnsignedLong("17440603504673385349"), 348 },
            { Long.parseUnsignedLong("12994262207056124023"), 375 },
            { Long.parseUnsignedLong("9681479787123295682"), 402 },
            { Long.parseUnsignedLong("14426529090290212157"), 428 },
            { Long.parseUnsignedLong("10748601772107342003"), 455 },
            { Long.parseUnsignedLong("16016664761464807395"), 481 },
            { Long.parseUnsignedLong("11933345169920330789"), 508 },
            { Long.parseUnsignedLong("17782069995880619868"), 534 },
            { Long.parseUnsignedLong("13248674568444952270"), 561 },
            { Long.parseUnsignedLong("9871031767461413346"), 588 },
            { Long.parseUnsignedLong("14708983551653345445"), 614 },
            { Long.parseUnsignedLong("10959046745042015199"), 641 },
            { Long.parseUnsignedLong("16330252207878254650"), 667 },
            { Long.parseUnsignedLong("12166986024289022870"), 694 },
            { Long.parseUnsignedLong("18130221999122236476"), 720 },
            { Long.parseUnsignedLong("13508068024458167312"), 747 },
            { Long.parseUnsignedLong("10064294952495520794"), 774 },
            { Long.parseUnsignedLong("14996968138956309548"), 800 },
            { Long.parseUnsignedLong("11173611982879273257"), 827 },
            { Long.parseUnsignedLong("16649979327439178909"), 853 },
            { Long.parseUnsignedLong("12405201291620119593"), 880 },
            { Long.parseUnsignedLong("9242595204427927429"), 907 },
            { Long.parseUnsignedLong("13772540099066387757"), 933 },
            { Long.parseUnsignedLong("10261342003245940623"), 960 },
            { Long.parseUnsignedLong("15290591125556738113"), 986 },
            { Long.parseUnsignedLong("11392378155556871081"), 1013 },
            { Long.parseUnsignedLong("16975966327722178521"), 1039 },
            { Long.parseUnsignedLong("12648080533535911531"), 1066 }
    };

    private FpconvDtoa() {
    }

    public static String d2string(double value) {
        if (Double.isNaN(value)) {
            return "nan";
        }
        if (Double.isInfinite(value)) {
            return value < 0.0d ? "-inf" : "inf";
        }
        if (value == 0.0d) {
            return Double.doubleToRawLongBits(value) < 0 ? "-0" : "0";
        }
        // double2ll：绝对值不超过 LLONG_MAX/2，且本身就是整数时，直接印整数。
        double half = (double) (Long.MAX_VALUE / 2);
        if (value >= -half && value <= half) {
            long asLong = (long) value;
            if (asLong == value) {
                return Long.toString(asLong);
            }
        }
        return fpconvDtoa(value);
    }

    private static String fpconvDtoa(double value) {
        StringBuilder dest = new StringBuilder(24);
        boolean negative = (Double.doubleToRawLongBits(value) & SIGN_MASK) != 0;
        if (negative) {
            dest.append('-');
            value = -value;
        }
        char[] digits = new char[18];
        int[] kHolder = new int[1];
        int count = grisu2(value, digits, kHolder);
        emitDigits(digits, count, dest, kHolder[0], negative);
        return dest.toString();
    }

    private static int grisu2(double value, char[] digits, int[] kHolder) {
        Fp w = buildFp(value);
        Fp[] bounds = normalizedBoundaries(w);
        Fp lower = bounds[0];
        Fp upper = bounds[1];
        w = normalize(w);
        int[] kOut = new int[1];
        Fp cp = findCachedPow10(upper.exp, kOut);
        w = multiply(w, cp);
        upper = multiply(upper, cp);
        lower = multiply(lower, cp);
        lower = new Fp(lower.frac + 1, lower.exp);
        upper = new Fp(upper.frac - 1, upper.exp);
        kHolder[0] = -kOut[0];
        return generateDigits(w, upper, lower, digits, kHolder);
    }

    private static Fp buildFp(double value) {
        long bits = Double.doubleToRawLongBits(value);
        long frac = bits & FRAC_MASK;
        int exp = (int) ((bits & EXP_MASK) >>> 52);
        if (exp != 0) {
            frac += HIDDEN_BIT;
            exp -= EXP_BIAS;
        } else {
            exp = -EXP_BIAS + 1;
        }
        return new Fp(frac, exp);
    }

    private static Fp normalize(Fp fp) {
        long frac = fp.frac;
        int exp = fp.exp;
        while ((frac & HIDDEN_BIT) == 0) {
            frac <<= 1;
            exp--;
        }
        int shift = 64 - 52 - 1;
        frac <<= shift;
        exp -= shift;
        return new Fp(frac, exp);
    }

    private static Fp[] normalizedBoundaries(Fp fp) {
        long upperFrac = (fp.frac << 1) + 1;
        int upperExp = fp.exp - 1;
        while ((upperFrac & (HIDDEN_BIT << 1)) == 0) {
            upperFrac <<= 1;
            upperExp--;
        }
        int uShift = 64 - 52 - 2;
        upperFrac <<= uShift;
        upperExp -= uShift;

        int lShift = fp.frac == HIDDEN_BIT ? 2 : 1;
        long lowerFrac = (fp.frac << lShift) - 1;
        int lowerExp = fp.exp - lShift;
        lowerFrac <<= lowerExp - upperExp;
        lowerExp = upperExp;
        return new Fp[] { new Fp(lowerFrac, lowerExp), new Fp(upperFrac, upperExp) };
    }

    private static Fp multiply(Fp a, Fp b) {
        long loMask = 0xFFFFFFFFL;
        long aHi = a.frac >>> 32;
        long aLo = a.frac & loMask;
        long bHi = b.frac >>> 32;
        long bLo = b.frac & loMask;
        long ahBl = aHi * bLo;
        long alBh = aLo * bHi;
        long alBl = aLo * bLo;
        long ahBh = aHi * bHi;
        long tmp = (ahBl & loMask) + (alBh & loMask) + (alBl >>> 32);
        tmp += 1L << 31;
        long frac = ahBh + (ahBl >>> 32) + (alBh >>> 32) + (tmp >>> 32);
        return new Fp(frac, a.exp + b.exp + 64);
    }

    private static int generateDigits(Fp fp, Fp upper, Fp lower, char[] digits, int[] kHolder) {
        long wfrac = upper.frac - fp.frac;
        long delta = upper.frac - lower.frac;
        int oneExp = upper.exp;
        long oneFrac = 1L << -oneExp;
        long part1 = upper.frac >>> -oneExp;
        long part2 = upper.frac & (oneFrac - 1);

        int idx = 0;
        int kappa = 10;
        for (int divIndex = 10; kappa > 0; divIndex++) {
            long div = TENS[divIndex];
            int digit = (int) Long.divideUnsigned(part1, div);
            if (digit != 0 || idx != 0) {
                digits[idx++] = (char) ('0' + digit);
            }
            part1 -= digit * div;
            kappa--;
            long tmp = (part1 << -oneExp) + part2;
            if (Long.compareUnsigned(tmp, delta) <= 0) {
                kHolder[0] += kappa;
                roundDigit(digits, idx, delta, tmp, div << -oneExp, wfrac);
                return idx;
            }
        }

        int unit = 18;
        while (true) {
            part2 *= 10;
            delta *= 10;
            kappa--;
            int digit = (int) (part2 >>> -oneExp);
            if (digit != 0 || idx != 0) {
                digits[idx++] = (char) ('0' + digit);
            }
            part2 &= oneFrac - 1;
            if (Long.compareUnsigned(part2, delta) < 0) {
                kHolder[0] += kappa;
                roundDigit(digits, idx, delta, part2, oneFrac, wfrac * TENS[unit]);
                return idx;
            }
            unit--;
        }
    }

    private static void roundDigit(char[] digits, int nDigits, long delta, long rem, long kappa, long frac) {
        while (Long.compareUnsigned(rem, frac) < 0
                && Long.compareUnsigned(delta - rem, kappa) >= 0
                && (Long.compareUnsigned(rem + kappa, frac) < 0
                || Long.compareUnsigned(frac - rem, rem + kappa - frac) > 0)) {
            digits[nDigits - 1]--;
            rem += kappa;
        }
    }

    private static void emitDigits(char[] digits, int nDigits, StringBuilder dest, int k, boolean negative) {
        int exp = Math.abs(k + nDigits - 1);
        if (k >= 0 && exp < (nDigits + 7)) {
            dest.append(digits, 0, nDigits);
            dest.append("0".repeat(k));
            return;
        }
        if (k < 0 && (k > -7 || exp < 4)) {
            int offset = nDigits - Math.abs(k);
            if (offset <= 0) {
                offset = -offset;
                dest.append('0');
                dest.append('.');
                dest.append("0".repeat(offset));
                dest.append(digits, 0, nDigits);
            } else {
                dest.append(digits, 0, offset);
                dest.append('.');
                dest.append(digits, offset, nDigits - offset);
            }
            return;
        }
        int limited = Math.min(nDigits, 18 - (negative ? 1 : 0));
        dest.append(digits[0]);
        if (limited > 1) {
            dest.append('.');
            dest.append(digits, 1, limited - 1);
        }
        dest.append('e');
        int exponent = k + nDigits - 1;
        dest.append(exponent < 0 ? '-' : '+');
        int magnitude = Math.abs(exponent);
        int cent = 0;
        if (magnitude > 99) {
            cent = magnitude / 100;
            dest.append((char) ('0' + cent));
            magnitude -= cent * 100;
        }
        if (magnitude > 9) {
            int dec = magnitude / 10;
            dest.append((char) ('0' + dec));
            magnitude -= dec * 10;
        } else if (cent != 0) {
            dest.append('0');
        }
        dest.append((char) ('0' + (magnitude % 10)));
    }

    private static Fp findCachedPow10(int exp, int[] kOut) {
        double oneLogTen = 0.30102999566398114;
        int approx = (int) (-(exp + N_POWERS) * oneLogTen);
        int idx = (approx - FIRST_POWER) / STEP_POWERS;
        while (true) {
            int current = exp + (int) POWERS_TEN[idx][1] + 64;
            if (current < EXP_MIN) {
                idx++;
                continue;
            }
            if (current > EXP_MAX) {
                idx--;
                continue;
            }
            kOut[0] = FIRST_POWER + idx * STEP_POWERS;
            return new Fp(POWERS_TEN[idx][0], (int) POWERS_TEN[idx][1]);
        }
    }

    private record Fp(long frac, int exp) {
    }
}

/*
 * The Grisu2 conversion and power table are derived from Redis deps/fpconv.
 * Boost Software License - Version 1.0 - August 17th, 2003
 *
 * Permission is hereby granted, free of charge, to any person or organization
 * obtaining a copy of the software and accompanying documentation covered by
 * this license (the "Software") to use, reproduce, display, distribute,
 * execute, and transmit the Software, and to prepare derivative works of the
 * Software, and to permit third-parties to whom the Software is furnished to
 * do so, all subject to the following:
 *
 * The copyright notices in the Software and this entire statement, including
 * the above license grant, this restriction and the following disclaimer,
 * must be included in all copies of the Software, in whole or in part, and
 * all derivative works of the Software, unless such copies or derivative
 * works are solely in the form of machine-executable object code generated by
 * a source language processor.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE, TITLE AND NON-INFRINGEMENT. IN NO EVENT
 * SHALL THE COPYRIGHT HOLDERS OR ANYONE DISTRIBUTING THE SOFTWARE BE LIABLE
 * FOR ANY DAMAGES OR OTHER LIABILITY, WHETHER IN CONTRACT, TORT OR OTHERWISE,
 * ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 */

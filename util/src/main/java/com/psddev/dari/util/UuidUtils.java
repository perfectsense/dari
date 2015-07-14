package com.psddev.dari.util;

import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.UUID;

import com.google.common.base.Preconditions;

/** {@link UUID} utility methods. */
public final class UuidUtils {

    /** Zero-filled byte array that matches the size of an UUID. */
    public static final byte[] ZERO_BYTES = new byte[16];

    /** Zero-filled UUID. */
    public static final UUID ZERO_UUID = new UUID(0L, 0L);

    /** Creates a sequential UUID. */
    public static synchronized UUID createSequentialUuid() {
        long time = System.currentTimeMillis();

        if (time < lastTime) {
            time = lastTime;
        }

        if (time == lastTime) {
            if (sequence == 0xffff) {
                ++ time;
                lastTime = time;
                sequence = 0;

            } else {
                ++ sequence;
            }

        } else {
            lastTime = time;
            sequence = 0;
        }

        return new UUID(
                (time & 0xffffffffffff0000L) | NODE_HIGH,
                (time << 16) | NODE_LOW | sequence);

    }

    private static long lastTime = System.currentTimeMillis();
    private static int sequence;

    private static final long NODE_HIGH;
    private static final long NODE_LOW;

    static {
        SecureRandom random = new SecureRandom();
        NODE_HIGH = 0xd000L | (random.nextLong() & 0xfff);
        NODE_LOW = 0xa000000000000000L | (random.nextLong() & 0x0fffffff00000000L);
    }

    /**
     * Returns the timestamp embedded within the given sequential {@code UUID}.
     *
     * @param uuid Can't be {@code null}.
     */
    public static long sequentialUuidTimestamp(UUID uuid) {
        return ((uuid.getMostSignificantBits() >> 16) << 16) + ((uuid.getLeastSignificantBits() >> 16) & 0xffff);
    }

    /**
     * Creates a version 3 (MD5 hash) UUID based on the given {@code name}.
     *
     * @param name Can't be {@code null}.
     * @return Never {@code null}.
     */
    public static UUID createVersion3Uuid(String name) {
        Preconditions.checkNotNull(name);

        byte[] md5 = StringUtils.md5(name);
        md5[6] = (byte) ((md5[6] & 0x0F) | 0x30);
        md5[8] = (byte) ((md5[8] & 0x0F) | 0x80);

        return fromBytes(md5);
    }

    /**
     * Converts the given {@code uuid} into a byte array.
     *
     * @throws UuidFormatException If the given {@code uuid} is {@code null}.
     */
    public static byte[] toBytes(UUID uuid) {
        if (uuid == null) {
            throw new UuidFormatException(
                    "Can't convert a null into a byte array!");
        }

        byte[] bytes = new byte[16];
        long msb = uuid.getMostSignificantBits();
        long lsb = uuid.getLeastSignificantBits();

        for (int i = 0; i < 8; i ++) {
            bytes[i] = (byte) (msb >>> 8 * (7 - i));
        }

        for (int i = 8; i < 16; i ++) {
            bytes[i] = (byte) (lsb >>> 8 * (7 - i));
        }

        return bytes;
    }

    /**
     * Converts the given {@code bytes} into an UUID.
     *
     * @throws UuidFormatException If the given byte array is {@code null}
     * or its length is not 16.
     */
    public static UUID fromBytes(byte[] bytes) {
        if (bytes == null) {
            throw new UuidFormatException(
                    "Can't convert a null into an UUID!");
        }

        if (bytes.length != 16) {
            throw new UuidFormatException(
                    "Can't convert the byte array into an UUID because its length is not 16!");
        }

        long msb = 0;
        long lsb = 0;

        for (int i = 0; i < 8; i ++) {
            msb = (msb << 8) | (bytes[i] & 0xff);
        }

        for (int i = 8; i < 16; i ++) {
            lsb = (lsb << 8) | (bytes[i] & 0xff);
        }

        return new UUID(msb, lsb);
    }

    private static final int[] HEX_CHARACTERS;

    static {
        int length = Byte.MAX_VALUE - Byte.MIN_VALUE;
        HEX_CHARACTERS = new int[length];

        for (int i = 0; i < length; ++ i) {
            HEX_CHARACTERS[i] = Character.digit(i, 16);
        }
    }

    /**
     * Converts the given {@code string} into an UUID.
     *
     * @param string Unlike {@link UUID#fromString}, dashes may be omitted.
     * @throws UuidFormatException If the given string is {@code null}
     * or isn't a valid UUID.
     */
    public static UUID fromString(String string) {
        if (string == null) {
            throw new UuidFormatException("Can't convert a null into an UUID!");
        }

        int length = string.length();

        if (length == 32 || length == 36) {
            byte[] letters = string.getBytes(StandardCharsets.UTF_8);
            int read = 0;
            int letterIndex = 0;
            int letterDigit;

            long msb = 0;
            long lsb = 0;

            for (; read < 16 && letterIndex < length; ++ letterIndex) {
                letterDigit = HEX_CHARACTERS[letters[letterIndex]];

                if (letterDigit >= 0) {
                    msb = (msb << 4) | letterDigit;
                    ++ read;
                }
            }

            if (read == 16) {
                for (; read < 32 && letterIndex < length; ++ letterIndex) {
                    letterDigit = HEX_CHARACTERS[letters[letterIndex]];

                    if (letterDigit >= 0) {
                        lsb = (lsb << 4) | letterDigit;
                        ++ read;
                    }
                }

                if (read == 32) {
                    return new UUID(msb, lsb);
                }
            }
        }

        throw new UuidFormatException("[" + string + "] isn't a valid UUID!");
    }

    // --- Deprecated ---

    /**
     * Creates a time-based version 1 UUID.
     *
     * <p>To use this method, it must be able to access a special
     * <a href="http://johannburkard.de/software/uuid/">UUID library</a>.
     * If you use Maven, you should add the following dependency:</p>
     *
     * <blockquote><pre><code data-type="xml">{@literal
     *<dependency>
     *    <groupId>com.eaio.uuid</groupId>
     *    <artifactId>uuid</artifactId>
     *    <version>3.2</version>
     *</dependency>}</code></pre></blockquote>
     *
     * @deprecated Use the UUID library directy.
     */
    @Deprecated
    public static UUID createVersion1Uuid() {
        com.eaio.uuid.UUID uuid = new com.eaio.uuid.UUID();
        return new UUID(uuid.getTime(), uuid.getClockSeqAndNode());
    }
}

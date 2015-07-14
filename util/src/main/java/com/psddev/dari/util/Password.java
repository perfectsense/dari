package com.psddev.dari.util;

import java.security.SecureRandom;

/** Password that can be stored securely. */
public final class Password {

    public static final String DEFAULT_ALGORITHM = "SHA-512";

    private static final SecureRandom RANDOM = new SecureRandom();

    private String data;

    /** Creates an instance with the given internal {@code data}. */
    public static Password valueOf(String data) {
        Password password = new Password();
        password.data = data;
        return password;
    }

    /**
     * Creates an instance that hashes the given {@code password} using
     * the given {@code algorithm} and {@code salt}.
     *
     * @param algorithm If {@code null}, uses {@value #DEFAULT_ALGORITHM}.
     * @param salt If {@code null}, generates a random value.
     * @param password If {@code null}, uses an empty string.
     */
    public static Password createCustom(String algorithm, String salt, String password) {
        if (algorithm == null) {
            algorithm = DEFAULT_ALGORITHM;
        }

        if (salt == null) {
            byte[] saltBytes = new byte[8];
            RANDOM.nextBytes(saltBytes);
            salt = StringUtils.hex(saltBytes);
        }

        if (password == null) {
            password = "";
        }

        return valueOf(algorithm + ":" + salt + ":" + hash(algorithm, salt, password));
    }

    /**
     * Creates an instance that hashes the given {@code password} using
     * the the {@value #DEFAULT_ALGORITHM} algorithm and a random salt.
     */
    public static Password create(String password) {
        return createCustom(null, null, password);
    }

    /**
     * Creates an instance that validates the given {@code password} using
     * the given {@code policy} and hashes it using the given {@code algorithm}
     * and {@code salt}.
     *
     * @deprecated Use {@link #validateAndCreateCustom(UserPasswordPolicy, Object, String, String, String)} instead.
     * @throws PasswordException If anything's wrong with the given {@code password}.
     */
    @Deprecated
    public static Password validateAndCreateCustom(
            PasswordPolicy policy,
            String algorithm,
            String salt,
            String password)
            throws PasswordException {

        if (password == null) {
            password = "";
        }

        if (policy != null) {
            policy.validate(password);
        }

        return createCustom(algorithm, salt, password);
    }

    /**
     * Creates an instance that validates the given {@code password}
     * for the given {@code user} using the given {@code policy} and
     * hashes it using the given {@code algorithm} and {@code salt}.
     *
     * @throws PasswordException If anything's wrong with the given {@code password}.
     */
    public static Password validateAndCreateCustom(
            UserPasswordPolicy policy,
            Object user,
            String algorithm,
            String salt,
            String password)
            throws PasswordException {

        if (password == null) {
            password = "";
        }

        if (policy != null) {
            policy.validate(user, password);
        }

        return createCustom(algorithm, salt, password);
    }

    /**
     * Hashes the given {@code algorithm}, {@code salt},
     * and {@code password}.
     */
    private static String hash(String algorithm, String salt, String password) {
        return StringUtils.hex(StringUtils.hash(algorithm, salt + password));
    }

    /** Creates a blank instance. */
    private Password() {
    }

    /**
     * Checks this password against the given {@code string} and returns
     * {@code true} if they're the same.
     */
    public boolean check(String string) {
        String[] dataParts = parseData();

        if (dataParts == null) {
            return false;
        }

        String algorithm = dataParts[0];
        String salt = dataParts[1];
        String hash = dataParts[2];

        return hash.equals(hash(algorithm, salt, string));
    }

    public String getAlgorithm() {
        String[] parsedData = parseData();
        return parsedData == null ? null : parsedData[0];
    }

    public String getSalt() {
        String[] parsedData = parseData();
        return parsedData == null ? null : parsedData[1];
    }

    // --- Object support ---

    @Override
    public boolean equals(Object other) {
        return this == other
                || (other instanceof Password
                && data.equals(((Password) other).data));
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }

    @Override
    public String toString() {
        return data;
    }

    /* Decomposes the password data and returns an array of size 3 containing
     * the algorithm, salt, and hash respectively. */
    private String[] parseData() {
        if (data == null) {
            return null;
        }

        String algorithm = "SHA-1";
        String salt = "";
        String hash = data;

        int colon1At = data.indexOf(':');
        if (colon1At >= 0) {
            int colon2At = data.indexOf(':', colon1At + 1);
            if (colon2At >= 0) {
                algorithm = data.substring(0, colon1At);
                salt = data.substring(colon1At + 1, colon2At);
                hash = data.substring(colon2At + 1);
            }
        }

        String[] dataParts = new String[3];
        dataParts[0] = algorithm;
        dataParts[1] = salt;
        dataParts[2] = hash;

        return dataParts;
    }
}

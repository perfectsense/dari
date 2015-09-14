package com.psddev.dari.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import javax.naming.AuthenticationException;
import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.StartTlsRequest;
import javax.naming.ldap.StartTlsResponse;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;

/**
 * LDAP utility methods.
 *
 * <p>All methods require proper configuration through {@link Settings}.</p>
 */
public class LdapUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(LdapUtils.class);

    /**
     * LDAP server URL.
     */
    public static final String PROVIDER_URL_SETTING = "dari/ldapProviderUrl";

    /**
     * Principal look-up format.
     * For example, {@code uid=%s,ou=people,dc=psddev,dc=com}.
     */
    public static final String PRINCIPAL_FORMAT_SETTING = "dari/ldapPrincipalFormat";

    /**
     * Custom CA certificate.
     */
    public static final String CUSTOM_CA_CERTIFICATE_SETTING = "dari/ldapCustomCaCertificate";

    /**
     * Path to the custom CA certificate.
     */
    public static final String CUSTOM_CA_CERTIFICATE_PATH_SETTING = "dari/ldapCustomCaCertificatePath";

    /**
     * Creates an initial context to the LDAP server.
     *
     * @return {@code null} if not configured.
     * @see #PROVIDER_URL_SETTING
     */
    public static LdapContext createContext() {
        String providerUrl = Settings.get(String.class, PROVIDER_URL_SETTING);

        if (ObjectUtils.isBlank(providerUrl)) {
            return null;
        }

        Properties env = new Properties();

        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, providerUrl);

        try {
            return new InitialLdapContext(env, null);

        } catch (NamingException error) {
            LOGGER.warn("Can't connect to LDAP!", error);
            return null;
        }
    }

    /**
     * Authenticates the given {@code principal} with the given
     * {@code credentials} in the given {@code context}.
     *
     * @param context Can't be {@code null}.
     * @param principal May be {@code null}.
     * @param credentials May be {@code null}.
     * @return {@code true} if authenticated.
     * @see #PRINCIPAL_FORMAT_SETTING
     * @see #CUSTOM_CA_CERTIFICATE_SETTING
     * @see #CUSTOM_CA_CERTIFICATE_PATH_SETTING
     */
    public static boolean authenticate(LdapContext context, String principal, String credentials) {
        try {
            String principalFormat = Settings.get(String.class, PRINCIPAL_FORMAT_SETTING);

            if (ObjectUtils.isBlank(principalFormat)) {
                return false;
            }

            // Custom certificate authority?
            SSLSocketFactory socketFactory = null;
            String caCertString = Settings.get(String.class, CUSTOM_CA_CERTIFICATE_SETTING);

            if (ObjectUtils.isBlank(caCertString)) {
                String caCertPath = Settings.get(String.class, CUSTOM_CA_CERTIFICATE_PATH_SETTING);

                if (!ObjectUtils.isBlank(caCertPath)) {
                    caCertString = IoUtils.toString(new File(caCertPath.trim()), StandardCharsets.UTF_8);
                }
            }

            if (!ObjectUtils.isBlank(caCertString)) {

                // Clean up the certificate in case it's pasted wrong.
                List<String> caCertLines = new ArrayList<String>(Arrays.asList(caCertString.trim().split("\\s+")));

                caCertLines.add(0, caCertLines.remove(0) + " " + caCertLines.remove(0));
                caCertLines.add(caCertLines.remove(caCertLines.size() - 2) + " " + caCertLines.remove(caCertLines.size() - 1));

                caCertString = Joiner.on("\n").join(caCertLines);

                // Custom SSL socket factory based on the CA cert.
                KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
                InputStream caCertInput = new ByteArrayInputStream(caCertString.getBytes(StandardCharsets.UTF_8));
                Certificate caCert = CertificateFactory.getInstance("X.509").generateCertificate(caCertInput);
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                SSLContext sslContext = SSLContext.getInstance("TLS");

                keyStore.load(null, null);
                keyStore.setCertificateEntry("ca", caCert);
                tmf.init(keyStore);
                sslContext.init(null, tmf.getTrustManagers(), null);

                socketFactory = sslContext.getSocketFactory();
            }

            try {
                StartTlsResponse tls = (StartTlsResponse) context.extendedOperation(new StartTlsRequest());

                tls.negotiate(socketFactory);

                try {
                    principal = String.format(principalFormat, "\"" + principal.replace("\"", "\\\"")  + "\"");

                    context.addToEnvironment(Context.SECURITY_PRINCIPAL, principal);
                    context.addToEnvironment(Context.SECURITY_CREDENTIALS, credentials);

                    // Force the reconnect to really authenticate using SSL.
                    try {
                        context.reconnect(null);
                        return true;

                    } catch (AuthenticationException error) {
                        return false;
                    }

                } finally {
                    tls.close();
                }

            } finally {
                context.close();
            }

        } catch (Exception error) {
            LOGGER.warn("Can't read from LDAP!", error);
            return false;
        }
    }
}

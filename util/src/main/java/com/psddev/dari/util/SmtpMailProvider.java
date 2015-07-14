package com.psddev.dari.util;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Properties;

import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Provides STMP mail support **/
public class SmtpMailProvider extends AbstractMailProvider {

    private static final Logger LOGGER = LoggerFactory.getLogger(SmtpMailProvider.class);

    private String host;
    private int port = 25;
    private String username;
    private String password;

    private boolean useTls;
    private long tlsPort = 587;

    private boolean useSsl;
    private long sslPort = 465;

    public SmtpMailProvider() {
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public boolean isUseTls() {
        return useTls;
    }

    public void setUseTls(boolean useTls) {
        this.useTls = useTls;
    }

    public long getTlsPort() {
        return tlsPort;
    }

    public void setTlsPort(long tlsPort) {
        this.tlsPort = tlsPort;
    }

    public boolean isUseSsl() {
        return useSsl;
    }

    public void setUseSsl(boolean useSsl) {
        this.useSsl = useSsl;
    }

    public long getSslPort() {
        return sslPort;
    }

    public void setSslPort(long sslPort) {
        this.sslPort = sslPort;
    }

    // --- MailProvider support ---

    @Override
    public void send(MailMessage message) {
        if (StringUtils.isEmpty(host)) {
            String errorText = "SMTP Host can't be null!";
            LOGGER.error(errorText);
            throw new IllegalArgumentException(errorText);
        }

        if (message == null) {
            String errorText = "EmailMessage can't be null!";
            LOGGER.error(errorText);
            throw new IllegalArgumentException(errorText);
        }

        Session session;

        Properties props = new Properties();
        props.put("mail.transport.protocol", "smtp");
        props.put("mail.smtp.host", host);
        props.put("mail.smtp.port", port);

        if (useTls) {
            props.put("mail.smtp.starttls.enable", "true");
            props.put("mail.smtp.port", tlsPort);

        // Note - not really tested.
        } else if (useSsl) {
            props.put("mail.smtp.socketFactory.port", sslPort);
            props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
            props.put("mail.smtp.port", sslPort);
        }

        if (!StringUtils.isEmpty(username)
                && !StringUtils.isEmpty(password)) {
            props.put("mail.smtp.auth", "true");
            session = Session.getInstance(props, new javax.mail.Authenticator() {
                @Override
                protected PasswordAuthentication getPasswordAuthentication() {
                    return new PasswordAuthentication(username, password);
                }
            });

        } else {
            session = Session.getInstance(props);
        }

        try {
            Message mimeMessage = new MimeMessage(session);

            mimeMessage.setFrom(new InternetAddress(message.getFrom()));
            mimeMessage.setRecipients(Message.RecipientType.TO, InternetAddress.parse(message.getTo()));
            mimeMessage.setSubject(message.getSubject());

            if (!StringUtils.isEmpty(message.getReplyTo())) {
                mimeMessage.setReplyTo(InternetAddress.parse(message.getReplyTo()));
            }

            // Body, plain vs. html
            MimeMultipart multiPartContent = new MimeMultipart();

            if (!StringUtils.isEmpty(message.getBodyPlain())) {
                MimeBodyPart plain = new MimeBodyPart();
                plain.setText(message.getBodyPlain(), StandardCharsets.UTF_8.toString());
                multiPartContent.addBodyPart(plain);
            }

            if (!StringUtils.isEmpty(message.getBodyHtml())) {
                MimeBodyPart html = new MimeBodyPart();
                html.setContent(message.getBodyHtml(), "text/html; charset=" + StandardCharsets.UTF_8.toString());
                multiPartContent.addBodyPart(html);
                multiPartContent.setSubType("alternative");
            }

            mimeMessage.setContent(multiPartContent);

            Transport.send(mimeMessage);
            LOGGER.debug("Sent email to [{}] with subject [{}].",
                    message.getTo(), message.getSubject());

        } catch (MessagingException me) {
            LOGGER.warn("Failed to send: [{}]", me.getMessage());
            me.printStackTrace();
            throw new RuntimeException(me);
        }
    }

    // --- SettingsBackedObject support ---

    /**
     * Called to initialize this stmp mail provider using the given {@code settings}.
     */
    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        this.setHost(ObjectUtils.to(String.class, settings.get("host")));
        this.setUsername(ObjectUtils.to(String.class, settings.get("username")));
        this.setPassword(ObjectUtils.to(String.class, settings.get("password")));

        Integer port = ObjectUtils.to(Integer.class, settings.get("port"));

        if (port != null) {
            this.setPort(port);
        }

        Object useTls = settings.get("useTls");
        if (useTls != null) {
            this.setUseTls(ObjectUtils.to(Boolean.class, useTls));
            Object tlsPort = settings.get("tlsPort");
            if (tlsPort != null) {
                this.setTlsPort(ObjectUtils.to(Long.class, tlsPort));
            }
        }

        Object useSsl = settings.get("useSsl");
        if (useSsl != null) {
            this.setUseSsl(ObjectUtils.to(Boolean.class, useSsl));
            Object sslPort = settings.get("sslPort");
            if (sslPort != null) {
                this.setSslPort(ObjectUtils.to(Long.class, sslPort));
            }
        }
    }
}

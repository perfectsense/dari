package com.psddev.dari.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import javax.xml.bind.DatatypeConverter;

/**
 * {@link SmsProvider} backed by <a href="http://www.twilio.com/">Twilio</a>.
 */
public class TwilioSmsProvider implements SmsProvider {

    public static final String ACCOUNT_SID_SUB_SETTING = "accountSid";
    public static final String AUTH_TOKEN_SUB_SETTING = "authToken";
    public static final String DEFAULT_FROM_NUMBER_SUB_SETTING = "defaultFromNumber";

    private String accountSid;
    private String authToken;
    private String defaultFromNumber;

    public String getAccountSid() {
        return accountSid;
    }

    public void setAccountSid(String accountSid) {
        this.accountSid = accountSid;
    }

    public String getAuthToken() {
        return authToken;
    }

    public void setAuthToken(String authToken) {
        this.authToken = authToken;
    }

    public String getDefaultFromNumber() {
        return defaultFromNumber;
    }

    public void setDefaultFromNumber(String defaultFromNumber) {
        this.defaultFromNumber = defaultFromNumber;
    }

    @Override
    public void initialize(String settingsKey, Map<String, Object> settings) {
        setAccountSid(ObjectUtils.to(String.class, settings.get(ACCOUNT_SID_SUB_SETTING)));
        setAuthToken(ObjectUtils.to(String.class, settings.get(AUTH_TOKEN_SUB_SETTING)));
        setDefaultFromNumber(ObjectUtils.to(String.class, settings.get(DEFAULT_FROM_NUMBER_SUB_SETTING)));
    }

    @Override
    public void send(String fromNumber, String toNumber, String message) {
        if (ObjectUtils.isBlank(fromNumber)) {
            fromNumber = getDefaultFromNumber();
        }

        try {
            HttpURLConnection conn = (HttpURLConnection) new URL(
                    "https://api.twilio.com/2010-04-01/Accounts/"
                    + getAccountSid()
                    + "/SMS/Messages.json").openConnection();

            conn.setRequestMethod("POST");
            conn.setRequestProperty("Authorization", "Basic " + DatatypeConverter.printBase64Binary((getAccountSid() + ":" + getAuthToken()).getBytes(StandardCharsets.UTF_8)));
            conn.setDoOutput(true);

            OutputStream output = conn.getOutputStream();

            try {
                writeParameter(output, "From", fromNumber);
                output.write('&');
                writeParameter(output, "To", toNumber);
                output.write('&');
                writeParameter(output, "Body", message);

            } finally {
                output.close();
            }

            InputStream input = conn.getInputStream();

            try {
                IoUtils.toString(input, StandardCharsets.UTF_8);

            } finally {
                input.close();
            }

        } catch (IOException error) {
            error.printStackTrace();
        }
    }

    private void writeParameter(OutputStream output, String name, String value) throws IOException {
        output.write(StringUtils.encodeUri(name).getBytes(StandardCharsets.UTF_8));
        output.write('=');
        output.write(StringUtils.encodeUri(value).getBytes(StandardCharsets.UTF_8));
    }
}

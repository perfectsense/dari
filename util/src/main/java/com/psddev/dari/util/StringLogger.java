package com.psddev.dari.util;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MarkerIgnoringBase;
import org.slf4j.helpers.MessageFormatter;

/**
 * {@linkplain org.slf4j.Logger SLF4J logger} implementation that saves
 * the logs into a single string.
 *
 * @deprecated No replacement.
 */
@Deprecated
@SuppressWarnings("serial")
public class StringLogger extends MarkerIgnoringBase {

    public static final String DEBUG_LEVEL = "DEBUG";
    public static final String ERROR_LEVEL = "ERROR";
    public static final String INFO_LEVEL = "INFO";
    public static final String TRACE_LEVEL = "TRACE";
    public static final String WARN_LEVEL = "WARN";

    private PrintWriter printWriter;
    private StringWriter stringWriter;
    private long startTime;

    /** Creates a new instance. */
    public StringLogger() {
        reset();
    }

    /** Resets this logger to its initial state. */
    public void reset() {
        stringWriter = new StringWriter();
        printWriter = new PrintWriter(stringWriter);
        startTime = System.currentTimeMillis();
    }

    private void log(String level, String message, Throwable throwable) {

        printWriter.print(System.currentTimeMillis() - startTime);

        printWriter.print(" [");
        printWriter.print(Thread.currentThread().getName());
        printWriter.print("] ");

        printWriter.print(level);
        printWriter.print(" - ");

        printWriter.println(message);

        if (throwable != null) {
            throwable.printStackTrace(printWriter);
        }
    }

    private void log(String level, FormattingTuple tuple) {
        log(level, tuple.getMessage(), tuple.getThrowable());
    }

    // --- MarkerIgnoringBase support ---

    @Override
    public void debug(String msg) {
        log(DEBUG_LEVEL, msg, null);
    }

    @Override
    public void debug(String format, Object arg) {
        log(DEBUG_LEVEL, MessageFormatter.format(format, arg));
    }

    @Override
    public void debug(String format, Object[] argArray) {
        log(DEBUG_LEVEL, MessageFormatter.format(format, argArray));
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
        log(DEBUG_LEVEL, MessageFormatter.format(format, arg1, arg2));
    }

    @Override
    public void debug(String msg, Throwable t) {
        log(DEBUG_LEVEL, msg, t);
    }

    @Override
    public void error(String msg) {
        log(ERROR_LEVEL, msg, null);
    }

    @Override
    public void error(String format, Object arg) {
        log(ERROR_LEVEL, MessageFormatter.format(format, arg));
    }

    @Override
    public void error(String format, Object[] argArray) {
        log(ERROR_LEVEL, MessageFormatter.format(format, argArray));
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
        log(ERROR_LEVEL, MessageFormatter.format(format, arg1, arg2));
    }

    @Override
    public void error(String msg, Throwable t) {
        log(ERROR_LEVEL, msg, t);
    }

    @Override
    public String getName() {
        return null;
    }

    @Override
    public void info(String msg) {
        log(INFO_LEVEL, msg, null);
    }

    @Override
    public void info(String format, Object arg) {
        log(INFO_LEVEL, MessageFormatter.format(format, arg));
    }

    @Override
    public void info(String format, Object[] argArray) {
        log(INFO_LEVEL, MessageFormatter.format(format, argArray));
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        log(INFO_LEVEL, MessageFormatter.format(format, arg1, arg2));
    }

    @Override
    public void info(String msg, Throwable t) {
        log(INFO_LEVEL, msg, t);
    }

    @Override
    public boolean isDebugEnabled() {
        return true;
    }

    @Override
    public boolean isErrorEnabled() {
        return true;
    }

    @Override
    public boolean isInfoEnabled() {
        return true;
    }

    @Override
    public boolean isTraceEnabled() {
        return true;
    }

    @Override
    public boolean isWarnEnabled() {
        return true;
    }

    @Override
    public void trace(String msg) {
        log(TRACE_LEVEL, msg, null);
    }

    @Override
    public void trace(String format, Object arg) {
        log(TRACE_LEVEL, MessageFormatter.format(format, arg));
    }

    @Override
    public void trace(String format, Object[] argArray) {
        log(TRACE_LEVEL, MessageFormatter.format(format, argArray));
    }

    @Override
    public void trace(String format, Object arg1, Object arg2) {
        log(TRACE_LEVEL, MessageFormatter.format(format, arg1, arg2));
    }

    @Override
    public void trace(String msg, Throwable t) {
        log(TRACE_LEVEL, msg, t);
    }

    @Override
    public void warn(String msg) {
        log(WARN_LEVEL, msg, null);
    }

    @Override
    public void warn(String format, Object arg) {
        log(WARN_LEVEL, MessageFormatter.format(format, arg));
    }

    @Override
    public void warn(String format, Object[] argArray) {
        log(WARN_LEVEL, MessageFormatter.format(format, argArray));
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
        log(WARN_LEVEL, MessageFormatter.format(format, arg1, arg2));
    }

    @Override
    public void warn(String msg, Throwable t) {
        log(WARN_LEVEL, msg, t);
    }

    // --- Object support ---

    @Override
    public String toString() {
        return stringWriter.toString();
    }
}

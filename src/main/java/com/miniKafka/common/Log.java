package com.minikafka.common;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Tiny structured logger. Kept intentionally dependency-free (no SLF4J/Logback) so the whole
 * project runs on the pure JDK. Output format: {@code TIME LEVEL [thread] source - message}.
 */
public final class Log {
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    private final String source;

    private Log(String source) {
        this.source = source;
    }

    public static Log of(Class<?> clazz) {
        return new Log(clazz.getSimpleName());
    }

    public void info(String msg, Object... args) {
        print("INFO", format(msg, args), null);
    }

    public void warn(String msg, Object... args) {
        print("WARN", format(msg, args), null);
    }

    public void error(String msg, Throwable t) {
        print("ERROR", t, t);
    }

    public void error(String msg, Object... args) {
        print("ERROR", format(msg, args), null);
    }

    private void print(String level, String msg, Throwable t) {
        String line = String.format("%s %s [%s] %s - %s",
                LocalDateTime.now().format(TS), level,
                Thread.currentThread().getName(), source, msg);

        if (level.startsWith("ERROR")) {
            System.err.println(line);
            if (t != null) {
                t.printStackTrace(System.err);
            }
        } else {
            System.out.println(line);
        }
    }

    /** Minimal {@code {}} placeholder substitution, like SLF4J. */
    private static String format(String msg, Object... args) {
        if (args == null || args.length == 0) {
            return msg;
        }

        StringBuilder sb = new StringBuilder(msg.length() + 16 * args.length);
        int argIdx = 0;
        int i = 0;

        while (i < msg.length()) {
            if (argIdx < args.length && i + 1 < msg.length()
                    && msg.charAt(i) == '{' && msg.charAt(i + 1) == '}') {
                sb.append(String.valueOf(args[argIdx++]));
                i += 2;
            } else {
                sb.append(msg.charAt(i));
                i++;
            }
        }

        return sb.toString();
    }
}
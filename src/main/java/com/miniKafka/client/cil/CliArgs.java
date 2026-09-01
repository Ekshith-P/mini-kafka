package com.minikafka.client.cli;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Very small {@code --key value} / {@code --flag} command-line parser for the CLI tools. */
final class CliArgs {
    private final Map<String, String> values = new HashMap<>();
    private final Set<String> flags = new HashSet<>();

    static CliArgs parse(String[] args) {
        CliArgs parsed = new CliArgs();
        int i = 0;
        while (i < args.length) {
            String token = args[i];
            if (token.startsWith("--")) {
                String key = token.substring(2);
                if (i + 1 < args.length && !args[i + 1].startsWith("--")) {
                    parsed.values.put(key, args[i + 1]);
                    i += 2;
                } else {
                    parsed.flags.add(key);
                    i += 1;
                }
            } else {
                i += 1;
            }
        }
        return parsed;
    }

    String get(String key, String defaultValue) {
        return values.getOrDefault(key, defaultValue);
    }

    String require(String key) {
        String v = values.get(key);
        if (v == null) {
            throw new IllegalArgumentException("missing required argument --" + key);
        }
        return v;
    }

    int getInt(String key, int defaultValue) {
        String v = values.get(key);
        return v == null ? defaultValue : Integer.parseInt(v);
    }

    boolean has(String flag) {
        return flags.contains(flag) || values.containsKey(flag);
    }
}
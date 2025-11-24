package com.streamforge.pipeline.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public final class AnomalyLogger {

    private static final Logger LOGGER = Logger.getLogger("AnomalyLogger");
    private static boolean initialized = false;

    private AnomalyLogger() {
    }

    public static Logger get() {
        if (!initialized) {
            setup();
        }
        return LOGGER;
    }

    private static synchronized void setup() {
        if (initialized) {
            return;
        }
        try {
            Path logsDir = Path.of("logs");
            Files.createDirectories(logsDir);
            Path logFile = logsDir.resolve("anomalies.log");
            FileHandler handler = new FileHandler(logFile.toString(), true);
            handler.setFormatter(new SimpleFormatter());
            LOGGER.addHandler(handler);
            LOGGER.setUseParentHandlers(false);
            LOGGER.setLevel(Level.INFO);
            initialized = true;
        } catch (IOException e) {
            throw new IllegalStateException("无法初始化异常日志: " + e.getMessage(), e);
        }
    }
}


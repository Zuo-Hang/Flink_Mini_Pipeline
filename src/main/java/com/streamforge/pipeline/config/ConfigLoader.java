package com.streamforge.pipeline.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public final class ConfigLoader {

    private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

    private ConfigLoader() {
    }

    public static KafkaClientConfig loadKafkaConfig(String path) throws IOException {
        return readYaml(path, KafkaClientConfig.class);
    }

    public static ClickHouseConfig loadClickHouseConfig(String path) throws IOException {
        return readYaml(path, ClickHouseConfig.class);
    }

    private static <T> T readYaml(String path, Class<T> target) throws IOException {
        Path file = Path.of(path);
        if (!Files.exists(file)) {
            throw new IOException("配置文件不存在: " + path);
        }
        return YAML_MAPPER.readValue(file.toFile(), target);
    }
}


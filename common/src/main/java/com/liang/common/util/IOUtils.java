package com.liang.common.util;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStream;
import java.io.PrintWriter;
import java.util.List;

@Slf4j
public class IOUtils {
    private IOUtils() {
    }

    public static InputStream getInStreamFromResource(String fileName) {
        return IOUtils.class.getClassLoader().getResourceAsStream(fileName);
    }

    @SneakyThrows
    public static InputStream getInStreamFromDisk(String path) {
        return new FileInputStream(path);
    }

    @SneakyThrows
    public static void writeInto(List<String> lines, String path, boolean append) {
        try (FileWriter fileWriter = new FileWriter(path, append); PrintWriter printerWriter = new PrintWriter(fileWriter, true)) {
            lines.forEach(printerWriter::println);
        }
    }
}

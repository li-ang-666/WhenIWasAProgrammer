package com.liang.common.service;

import com.obs.services.ObsClient;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ObsWriter {
    private final static int DEFAULT_CACHE_TIME = 1000 * 60 * 10; //10min
    private final static int DEFAULT_CACHE_SIZE = 100 * 1024 * 1024; //100mb
    private final static String ak = "NT5EWZ4FRH54R2R2CB8G";
    private final static String sk = "BJok3jQFTmFYUS68lFWegazYggw5anKsOFUb65bS";
    private final static String ep = "obs.cn-north-4.myhuaweicloud.com";

    private final Cache cache = new Cache(DEFAULT_CACHE_SIZE);
    private final String bucket;
    private final String path;
    private final Logging logging;

    private volatile boolean enableCache = false;

    public ObsWriter(String folderPath) {
        bucket = folderPath.replaceAll("obs://(.*?)/(.*)", "$1");
        path = folderPath.replaceAll("obs://(.*?)/(.*)", "$2");
        logging = new Logging(this.getClass().getSimpleName(), folderPath);
    }

    public ObsWriter enableCache() {
        return enableCache(DEFAULT_CACHE_TIME);
    }

    public ObsWriter enableCache(int cacheTime) {
        if (!enableCache) {
            enableCache = true;
            new Thread(new Sender(this, cacheTime)).start();
        }
        return this;
    }

    public void println(String... contents) {
        if (contents == null || contents.length == 0) {
            return;
        }
        println(Arrays.asList(contents));
    }

    public void println(List<String> contents) {
        if (contents == null || contents.isEmpty()) {
            return;
        }
        for (String content : contents) {
            synchronized (cache) {
                cache.addLine(content);
                if (cache.size() >= DEFAULT_CACHE_SIZE) {
                    printlnImmediately();
                }
            }
        }
        if (!enableCache) {
            printlnImmediately();
        }
    }

    private synchronized void printlnImmediately() {
        if (cache.isEmpty()) {
            return;
        }
        logging.beforeExecute();
        String path = this.path + (this.path.endsWith("/") ? "" : "/");
        String objectName = String.format("%s.%s.%s", System.currentTimeMillis(), UUID.randomUUID(), "txt");
        try (ObsClient client = new ObsClient(ak, sk, ep)) {
            client.putObject(bucket, path + objectName, cache.getInputStream());
            //只有写入成功才清空cache
            cache.clear();
            logging.afterExecute("write", objectName);
        } catch (Exception e) {
            logging.ifError("write", objectName, e);
        }
    }

    private static class Sender implements Runnable {
        private final ObsWriter obsWriter;
        private final int cacheTime;

        public Sender(ObsWriter obsWriter, int cacheTime) {
            this.obsWriter = obsWriter;
            this.cacheTime = cacheTime;
        }

        @Override
        @SneakyThrows
        @SuppressWarnings("InfiniteLoopStatement")
        public void run() {
            while (true) {
                TimeUnit.MILLISECONDS.sleep(cacheTime);
                if (obsWriter.cache.isEmpty()) {
                    continue;
                }
                synchronized (obsWriter.cache) {
                    if (obsWriter.cache.isEmpty()) {
                        continue;
                    }
                    obsWriter.printlnImmediately();
                }
            }
        }
    }

    private static class Cache {
        private final int initSize;
        private StringBuilder builder;

        public Cache(int initSize) {
            this.initSize = (int) (initSize * 1.2);
            builder = new StringBuilder(initSize);
        }

        public void addLine(String content) {
            builder.append(content);
            builder.append("\n");
        }

        public InputStream getInputStream() {
            return new ByteArrayInputStream(
                    builder.toString().getBytes(StandardCharsets.UTF_8));
        }

        public void clear() {
            builder = new StringBuilder(initSize);
        }

        public boolean isEmpty() {
            return builder.length() == 0;
        }

        public int size() {
            return builder.length();
        }
    }
}

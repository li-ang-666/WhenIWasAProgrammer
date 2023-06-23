package com.liang.common.service;

import com.liang.common.util.DateTimeUtils;
import com.obs.services.ObsClient;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Slf4j
public class ObsWriter {
    private final static int DEFAULT_CACHE_TIME = 1000 * 60 * 10; //10min
    private final static int DEFAULT_CACHE_SIZE = 100 * 1024 * 1024; //100mb
    private final static String ak = "NT5EWZ4FRH54R2R2CB8G";
    private final static String sk = "BJok3jQFTmFYUS68lFWegazYggw5anKsOFUb65bS";
    private final static String ep = "obs.cn-north-4.myhuaweicloud.com";

    private final List<Byte> cache = new ArrayList<>();
    private final String bucket;
    private final String path;

    private volatile boolean enableCache = false;

    public ObsWriter(String folderPath) {
        bucket = folderPath.replaceAll("obs://(.*?)/(.*)", "$1");
        path = folderPath.replaceAll("obs://(.*?)/(.*)", "$2");
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
                for (byte b : content.getBytes(StandardCharsets.UTF_8)) {
                    cache.add(b);
                }
                cache.add((byte) '\n');
                if (cache.size() >= DEFAULT_CACHE_SIZE) {
                    printlnImmediately(getStream(cache));
                    cache.clear();
                }
            }
        }
        if (!enableCache) {
            printlnImmediately(getStream(cache));
            cache.clear();
        }
    }

    private synchronized void printlnImmediately(InputStream inputStream) {
        long time = System.currentTimeMillis();
        String objectName = DateTimeUtils.fromUnixTime(time / 1000, "yyyyMMdd-HHmmss");
        try (ObsClient client = new ObsClient(ak, sk, ep)) {
            client.putObject(bucket, path + "/" + objectName, inputStream);
        } catch (Exception e) {
            log.error("obs write error", e);
        }
    }

    private InputStream getStream(List<Byte> cache) {
        byte[] bytes = new byte[cache.size()];
        for (int i = 0; i < cache.size(); i++) {
            bytes[i] = cache.get(i);
        }
        return new ByteArrayInputStream(bytes);
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
                    obsWriter.printlnImmediately(obsWriter.getStream(obsWriter.cache));
                    obsWriter.cache.clear();
                }
            }
        }
    }
}

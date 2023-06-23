package com.liang.common.service.filesystem;

import com.liang.common.service.AbstractCache;
import com.liang.common.service.Logging;
import com.obs.services.ObsClient;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

@Slf4j
public class ObsWriter extends AbstractCache<Object, String> {
    private final static int DEFAULT_CACHE_MILLISECONDS = 1000 * 60 * 10;
    private final static int DEFAULT_CACHE_RECORDS = 100 * 1024 * 1024;
    private final static String ACCESS_KEY = "NT5EWZ4FRH54R2R2CB8G";
    private final static String SECRET_KEY = "BJok3jQFTmFYUS68lFWegazYggw5anKsOFUb65bS";
    private final static String END_POINT = "obs.cn-north-4.myhuaweicloud.com";

    private final String bucket;
    private final String path;
    private final Logging logging;

    public ObsWriter(String folderPath) {
        super(DEFAULT_CACHE_MILLISECONDS, DEFAULT_CACHE_RECORDS, content -> null);
        bucket = folderPath.replaceAll("obs://(.*?)/(.*)", "$1");
        path = folderPath.replaceAll("obs://(.*?)/(.*)", "$2");
        logging = new Logging(this.getClass().getSimpleName(), folderPath);
    }

    @Override
    protected synchronized void updateImmediately(Object ignore, List<String> rows) {
        logging.beforeExecute();
        String path = this.path + (this.path.endsWith("/") ? "" : "/");
        String objectName = String.format("%s.%s.%s", System.currentTimeMillis(), UUID.randomUUID(), "txt");
        byte[] bytes = String.join("\n", rows).getBytes(StandardCharsets.UTF_8);
        try (ObsClient client = new ObsClient(ACCESS_KEY, SECRET_KEY, END_POINT)) {
            client.putObject(bucket, path + objectName, new ByteArrayInputStream(bytes));
            logging.afterExecute("write", objectName);
        } catch (Exception e) {
            logging.ifError("write", objectName, e);
        }
    }
}

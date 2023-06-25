package com.liang.common.service.filesystem;

import com.liang.common.service.AbstractCache;
import com.liang.common.service.Logging;
import com.liang.common.util.DateTimeUtils;
import com.obs.services.ObsClient;
import com.obs.services.model.ModifyObjectRequest;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

@Slf4j
public class ObsWriter extends AbstractCache<Object, String> {
    private final static int DEFAULT_CACHE_MILLISECONDS = 5000;
    private final static int DEFAULT_CACHE_RECORDS = 10240;
    private final static String ACCESS_KEY = "NT5EWZ4FRH54R2R2CB8G";
    private final static String SECRET_KEY = "BJok3jQFTmFYUS68lFWegazYggw5anKsOFUb65bS";
    private final static String END_POINT = "obs.cn-north-4.myhuaweicloud.com";

    private final UUID uuid = UUID.randomUUID();
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
    @Synchronized
    protected void updateImmediately(Object ignore, List<String> rows) {
        logging.beforeExecute();
        String path = this.path + (this.path.endsWith("/") ? "" : "/");
        String objectName = String.format("%s.%s.%s", DateTimeUtils.currentDate(), uuid, "txt");
        byte[] bytes = String.join("\n", rows).getBytes(StandardCharsets.UTF_8);
        try (ObsClient client = new ObsClient(ACCESS_KEY, SECRET_KEY, END_POINT)) {
            long position = 0L;
            try {
                position = client.getObjectMetadata(bucket, path + this).getNextPosition();
            } catch (Exception ignored) {
            }
            ModifyObjectRequest request = new ModifyObjectRequest();
            request.setBucketName(bucket);
            request.setObjectKey(path + this);
            request.setPosition(position);
            request.setInput(new ByteArrayInputStream(bytes));
            client.modifyObject(request);
            logging.afterExecute("write", objectName);
        } catch (Exception e) {
            logging.ifError("write", objectName, e);
        }
    }
}

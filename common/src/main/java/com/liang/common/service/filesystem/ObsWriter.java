package com.liang.common.service.filesystem;

import com.liang.common.service.AbstractCache;
import com.liang.common.service.Logging;
import com.liang.common.util.DateTimeUtils;
import com.obs.services.ObsClient;
import com.obs.services.model.ModifyObjectRequest;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class ObsWriter extends AbstractCache<String, String> {
    private final static int BUFFER_MAX_MB = 32;
    private final static int DEFAULT_CACHE_MILLISECONDS = 5000;
    private final static int DEFAULT_CACHE_RECORDS = 10240;
    private final static String ACCESS_KEY = "NT5EWZ4FRH54R2R2CB8G";
    private final static String SECRET_KEY = "BJok3jQFTmFYUS68lFWegazYggw5anKsOFUb65bS";
    private final static String END_POINT = "obs.cn-north-4.myhuaweicloud.com";

    private final ObsClient client = new ObsClient(ACCESS_KEY, SECRET_KEY, END_POINT);
    private final UUID uuid = UUID.randomUUID();
    private final Map<String, AtomicLong> fileToPosition = new HashMap<>();
    private final String bucket;
    private final String objectKeyPrefix;
    private final Logging logging;

    public ObsWriter(String folder) {
        super(BUFFER_MAX_MB, DEFAULT_CACHE_MILLISECONDS, DEFAULT_CACHE_RECORDS, content -> "");
        String[] formatFolder = folder
                .replaceAll("obs://(.*?)/(.*)", "$1\001$2")
                .split("\001");
        bucket = formatFolder[0];
        objectKeyPrefix = formatFolder[1] + (formatFolder[1].endsWith("/") ? "" : "/");
        logging = new Logging(this.getClass().getSimpleName(), folder);
    }

    @Override
    protected void updateImmediately(String ignore, Queue<String> rows) {
        logging.beforeExecute();
        String objectKeyName = String.format("%s.%s.%s", DateTimeUtils.currentDate(), uuid, "txt");
        try {
            String objectKey = objectKeyPrefix + objectKeyName;
            if (!fileToPosition.containsKey(objectKeyName)) {
                fileToPosition.clear();
                long position = 0L;
                try {
                    position = client.getObjectMetadata(bucket, objectKey).getNextPosition();
                } catch (Exception ignored) {
                }
                fileToPosition.put(objectKeyName, new AtomicLong(position));
            }
            AtomicLong position = fileToPosition.get(objectKeyName);
            ModifyObjectRequest request = new ModifyObjectRequest();
            request.setBucketName(bucket);
            request.setObjectKey(objectKey);
            byte[] bytes = (String.join("\n", rows) + "\n").getBytes(StandardCharsets.UTF_8);
            request.setInput(new ByteArrayInputStream(bytes));
            request.setPosition(position.getAndAdd(bytes.length));
            client.modifyObject(request);
            Object methodArg = rows.size() > 100 ? objectKeyName + "(" + rows.size() + "条)" : rows;
            logging.afterExecute("write", methodArg);
        } catch (Exception e) {
            logging.ifError("write", objectKeyName, e);
        }
    }
}

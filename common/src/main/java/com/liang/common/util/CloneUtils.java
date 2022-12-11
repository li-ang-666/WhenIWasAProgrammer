package com.liang.common.util;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

@Slf4j
@SuppressWarnings("unchecked")
public class CloneUtils {
    private CloneUtils() {
    }

    public static <T> T deepClone(T src) {
        Object obj;
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(byteArrayOutputStream);
            objectOutputStream.writeObject(src);
            objectOutputStream.close();
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(byteArrayOutputStream.toByteArray());
            ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream);
            obj = objectInputStream.readObject();
            objectInputStream.close();
        } catch (Exception e) {
            log.error("克隆异常, obj: {}", src, e);
            obj = null;
        }
        return (T) obj;
    }
}

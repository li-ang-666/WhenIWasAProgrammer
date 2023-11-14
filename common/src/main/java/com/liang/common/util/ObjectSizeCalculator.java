package com.liang.common.util;

import lombok.experimental.UtilityClass;
import org.openjdk.jol.info.GraphLayout;

@UtilityClass
public class ObjectSizeCalculator {
    public static long getObjectSize(Object obj) {
        return obj == null ? 0 : GraphLayout.parseInstance(obj).totalSize();
    }
}

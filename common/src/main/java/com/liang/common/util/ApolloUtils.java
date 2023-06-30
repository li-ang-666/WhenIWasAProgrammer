package com.liang.common.util;

import com.ctrip.framework.apollo.Config;
import com.ctrip.framework.apollo.ConfigChangeListener;
import com.ctrip.framework.apollo.ConfigService;
import com.ctrip.framework.apollo.model.ConfigChange;
import com.ctrip.framework.apollo.model.ConfigChangeEvent;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@UtilityClass
@Slf4j
// https://www.apolloconfig.com/#/zh/README
public class ApolloUtils {
    private static final Config apollo;

    static {
        // http://apollo.jindidata.com
        System.setProperty("apollo.meta", "http://apollo.middleware.huawei:8080");
        System.setProperty("app.id", "ApolloUtils");
        apollo = ConfigService.getAppConfig();
        apollo.addChangeListener(new ConfigChangeListener() {
            @Override
            public void onChange(ConfigChangeEvent configChangeEvent) {
                for (String key : configChangeEvent.changedKeys()) {
                    ConfigChange change = configChangeEvent.getChange(key);
                    log.info("Apollo: {} {}, {} -> {}", change.getPropertyName(), change.getChangeType(), change.getOldValue(), change.getNewValue());
                }
            }
        });
    }

    public static String get(String key) {
        return apollo.getProperty(key, null);
    }

    public static String getOrDefault(String key, String defaultValue) {
        return apollo.getProperty(key, defaultValue);
    }
}

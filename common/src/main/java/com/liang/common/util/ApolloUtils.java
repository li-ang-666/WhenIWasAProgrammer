package com.liang.common.util;

import com.ctrip.framework.apollo.ConfigService;
import lombok.experimental.UtilityClass;

@UtilityClass
public class ApolloUtils {
    public static void main(String[] args) {
        f();
    }

    public static void f() {
        System.setProperty("apollo.meta", "http://apollo.middleware.huawei:8080");
        System.setProperty("app.id", "config-apollo");
        System.out.println(ConfigService.getAppConfig().getPropertyNames());
    }
}

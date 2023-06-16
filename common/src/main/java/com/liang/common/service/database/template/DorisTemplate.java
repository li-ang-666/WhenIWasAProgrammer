package com.liang.common.service.database.template;

import com.liang.common.util.ConfigUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

@Slf4j
public class DorisTemplate {
    private final HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DefaultRedirectStrategy() {
                @Override
                protected boolean isRedirectable(String method) {
                    return true;
                }
            });

    public DorisTemplate(String name) {
        ConfigUtils.getConfig()

    }
}

package com.liang.common.service.database.template.doris;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.impl.client.DefaultRedirectStrategy;

@Slf4j
class DorisRedirectStrategy extends DefaultRedirectStrategy {
    @Override
    protected boolean isRedirectable(String method) {
        return true;
    }
}
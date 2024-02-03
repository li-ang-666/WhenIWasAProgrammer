package com.liang.common.service.database.template.doris;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.DefaultRedirectStrategy;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.net.URI;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class HttpPutExecutor {
    private static final int SUCCESS_STATUS_CODE = 200;
    private static final String SUCCESS_STATUS = "Success";
    private static final String SUCCESS_MESSAGE = "OK";
    private static final String PUBLISH_TIMEOUT_STATUS = "Publish Timeout";
    private static final String PUBLISH_TIMEOUT_MESSAGE = "PUBLISH_TIMEOUT";
    private static final int MAX_TRY_TIMES = 3;
    private static final int RETRY_INTERVAL_MILLISECOND = 1000;
    private final HttpClientBuilder httpClientBuilder = HttpClients
            .custom()
            .setRedirectStrategy(new DorisRedirectStrategy());

    public void execute(HttpPut put, URI uri, String label) {
        try (CloseableHttpClient client = httpClientBuilder.build()) {
            int tryTimes = MAX_TRY_TIMES;
            while (tryTimes-- > 0) {
                put.setURI(uri);
                put.setHeader("label", label);
                try (CloseableHttpResponse response = client.execute(put)) {
                    int statusCode = response.getStatusLine().getStatusCode();
                    String loadResult = EntityUtils.toString(response.getEntity());
                    if (statusCode == SUCCESS_STATUS_CODE && loadResult.contains(SUCCESS_STATUS) && loadResult.contains(SUCCESS_MESSAGE)) { // Status = Success, Message = OK
                        log.info("stream load success, loadResult:\n{}", loadResult);
                        tryTimes = 0;
                    } else if (statusCode == SUCCESS_STATUS_CODE && loadResult.contains(PUBLISH_TIMEOUT_STATUS) && loadResult.contains(PUBLISH_TIMEOUT_MESSAGE)) { // Status = Publish Timeout, Message = PUBLISH_TIMEOUT
                        log.warn("stream load success, loadResult:\n{}", loadResult);
                        tryTimes = 0;
                    } else if (tryTimes == 0) {
                        log.error("stream load failed for {} times, statusCode: {}, loadResult:\n{}", MAX_TRY_TIMES, statusCode, loadResult);
                    } else {
                        LockSupport.parkUntil(System.currentTimeMillis() + RETRY_INTERVAL_MILLISECOND);
                    }
                }
            }
        } catch (Exception e) {
            log.error("stream load failed without loadResult", e);
        }
    }

    @Slf4j
    private static final class DorisRedirectStrategy extends DefaultRedirectStrategy {
        @Override
        protected boolean isRedirectable(String method) {
            return true;
        }
    }
}

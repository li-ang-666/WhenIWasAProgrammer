package com.liang.repair.test;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.repair.service.ConfigHolder;
import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class RepairTest extends ConfigHolder {
    public static void main(String[] args) throws Exception {
        List<Callable<Void>> list = Arrays.asList(

                () -> {
                    new JdbcTemplate("435.company_base").streamQuery(false, "select id from company_index", rs -> {
                    });
                    return null;
                },
                () -> {
                    new JdbcTemplate("116.prism").streamQuery(false, "select id from equity_ratio", rs -> {
                    });
                    return null;
                }
                , () -> {
                    new JdbcTemplate("104.data_bid").streamQuery(false, "select id from company_bid", rs -> {
                    });
                    return null;
                }
        );
        ExecutorService executorService = Executors.newFixedThreadPool(3);
        executorService.invokeAll(list);
    }
}

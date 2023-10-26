package com.liang.repair.test;

import com.liang.repair.service.ConfigHolder;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.concurrent.TimeUnit;

@Slf4j
public class Test extends ConfigHolder {
    private final static long MIN = 1L;
    private final static long MAX = 56412887084662L;

    @SneakyThrows
    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection("jdbc:mysql://be4103034fe44e02a946cb7d543bba41in01.internal.cn-north-4.mysql.rds.myhuaweicloud.com:3306/data_es", "jdhw_d_data_dml", "2s0^tFa4SLrp72");
        for (long i = MIN; i <= MAX; i += 1024) {
            String sql = String.format("update company_patent_basic_info_index set update_time = date_add(update_time, interval 1 second) where %s <= id and id <= %s", i, i + 1024);
            log.info("sql: {}", sql);
            connection.prepareStatement(sql).executeUpdate();
            TimeUnit.SECONDS.sleep(3);
        }
    }
}

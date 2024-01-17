package com.liang.spark.job;

import com.liang.common.service.database.template.JdbcTemplate;
import com.liang.common.util.ConfigUtils;
import com.liang.spark.basic.SparkSessionFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;

@Slf4j
public class DorisCopyJob {
    public static void main(String[] args) {
        log.info("args: {}", Arrays.toString(args));
        SparkSession spark = SparkSessionFactory.createSpark(null);
        String source = args[0];
        log.info("source: {}", source);
        String sink = "bak." + source.split("\\.")[1];
        log.info("sink: {}", sink);
        JdbcTemplate doris = new JdbcTemplate("doris");
        // drop sink
        String dropSql = String.format("drop table if exists %s force", sink);
        doris.update(dropSql);
        // write sink
        String descSql = "desc " + source;
        String createSql = doris.queryForList(descSql, rs -> rs.getString(1))
                .stream()
                .map(e -> String.format("cast(%s as string) %s", e, e))
                .collect(Collectors.joining(", ", String.format("create table %s as select ", sink), String.format(" from %s", source)));
        doris.update(createSql);
        // check
        String sourceCount = doris.queryForObject("select count(1) from " + source, rs -> rs.getString(1));
        String sinkCount = doris.queryForObject("select count(1) from " + sink, rs -> rs.getString(1));
        while (!sourceCount.equals(sinkCount)) {
            log.warn("sourceCnt {} not eq sinkCnt {}", sourceCount, sinkCount);
            LockSupport.parkUntil(System.currentTimeMillis() + 1000 * 5);
        }
        // spark
        spark.read().format("doris")
                .option("doris.fenodes", "10.99.197.34:8030,10.99.202.71:8030,10.99.203.88:8030")
                .option("doris.table.identifier", sink)
                .option("user", "dba")
                .option("password", "Tyc@1234")
                .option("doris.batch.size", "10240")
                .option("doris.request.tablet.size", "1")
                .load()
                .repartition()
                .foreachPartition(new DorisJob.DorisSink(ConfigUtils.getConfig(), source.split("\\.")[0], source.split("\\.")[1]));
    }
}

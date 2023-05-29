package com.liang.spark.job;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;

/**
 * CREATE TABLE `moka_user_annual_title` (
 * `id` bigint(20) unsigned NOT NULL AUTO_INCREMENT COMMENT 'id',
 * `org_id` varchar(255) NOT NULL COMMENT '客户id',
 * `user_id` bigint(20) NOT NULL COMMENT '用户id',
 * `created_at` datetime NOT NULL COMMENT '用户注册时间',
 * `period` bigint(20) NOT NULL COMMENT '用户注册时间到12月1日的天数',
 * `title` varchar(255) NOT NULL COMMENT '用户昵称',
 * PRIMARY KEY (`id`),
 * KEY `idx_user` (`org_id`,`user_id`)
 * ) ENGINE=InnoDB AUTO_INCREMENT=26513 DEFAULT CHARSET=utf8 COMMENT='市场部年底账单'
 */
@Slf4j
public class Launcher {
    public static void main(String[] args) throws Exception {
        SparkSession ssc = SparkSession
                .builder()
                .config("spark.debug.maxToStringFields", "200")
                .enableHiveSupport()
                .getOrCreate();
        ssc.sql("use ods_bi_production_hive");
        ssc.udf().register("my_udf", new MyUDF(), DataTypes.StringType);
        //全平台
        String createInterviewTimes = ssc.sql("select " +
                "cast(cast(count(interview_arranger_id)/count(distinct interview_arranger_id) as int) as string) " +
                "from dwd_interviews where created_at between '2021-01-01 00:00:00' and '2021-12-01 23:59:59'")
                .collectAsList().stream().map(e -> e.getString(0)).collect(Collectors.toList()).get(0);
        log.warn("全平台平均create interview次数: " + createInterviewTimes);
        Tuple2<String, String> offerTimesAndHireTimes = ssc.sql("select " +
                "cast(cast(count(offered_by)/count(distinct offered_by) as int) as string)," +
                "cast(cast(count(hired_by)/count(distinct hired_by) as int) as string) " +
                "from dwd_applications where created_at between '2021-01-01 00:00:00' and '2021-12-01 23:59:59'")
                .collectAsList().stream().map(e -> Tuple2.of(e.getString(0), e.getString(1))).collect(Collectors.toList()).get(0);
        String sendOfferTimes = offerTimesAndHireTimes.f0;
        String hireTimes = offerTimesAndHireTimes.f1;
        log.warn("全平台平均发offer次数: " + sendOfferTimes);
        log.warn("全平台平均办理hire次数: " + hireTimes);
        //创建面试次数
        Dataset<Row> t1 = ssc.sql("select org_id,interview_arranger_id user_id,count(1) create_interview_times,0 send_offer_times,0 hire_times,0 login_times,0 19_times,0 23_times from dwd_interviews where created_at between '2021-01-01 00:00:00' and '2021-12-01 23:59:59' group by org_id,interview_arranger_id");
        //发送offer次数
        Dataset<Row> t2 = ssc.sql("select org_id,offered_by user_id,0 create_interview_times,count(1) send_offer_times, 0 hire_times,0 login_times,0 19_times,0 23_times from dwd_applications where offer_sent_at between '2021-01-01 00:00:00' and '2021-12-01 23:59:59' group by org_id,offered_by");
        //hire办理次数
        Dataset<Row> t3 = ssc.sql("select org_id,hired_by user_id,0 create_interview_times,0 send_offer_times,count(1) hire_times,0 login_times,0 19_times,0 23_times from dwd_applications where hired_at between '2021-01-01 00:00:00' and '2021-12-01 23:59:59' group by org_id,hired_by");
        //登陆天数
        Dataset<Row> t4 = ssc.sql("select org_id,user_id,0 create_interview_times,0 send_offer_times,0 hire_times,count(distinct substr(visited_at,0,10)) login_times,0 19_times,0 23_times from dwd_login_record where visited_at between '2021-01-01 00:00:00' and '2021-12-01 23:59:59' group by org_id,user_id");
        //19点、23点后有操作的天数
        Dataset<Row> t5 = ssc.sql("select org_id,operator_id user_id,0 create_interview_times,0 send_offer_times,0 hire_times,0 login_times," +
                "count(distinct if(substr(created_at,12,2) in ('19','20','21','22','23','00','01'),substr(created_at,0,10),null)) 19_times," +
                "count(distinct if(substr(created_at,12,2) in ('23','00','01'),substr(created_at,0,10),null)) 23_times " +
                "from dwd_activities where created_at between '2021-01-01 00:00:00' and '2021-12-01 23:59:59' group by org_id,operator_id");
        //union
        t1.where("user_id is not null")
                .unionAll(t2.where("user_id is not null"))
                .unionAll(t3.where("user_id is not null"))
                .unionAll(t3.where("user_id is not null"))
                .unionAll(t4.where("user_id is not null"))
                .unionAll(t5.where("user_id is not null"))
                .createTempView("t");
        //aggregate
        ssc.sql("select org_id,user_id," +
                "sum(create_interview_times) create_interview_times," +
                "sum(send_offer_times) send_offer_times," +
                "sum(hire_times) hire_times," +
                "sum(login_times) login_times," +
                "sum(19_times) 19_times," +
                "sum(23_times) 23_times " +
                "from t group by org_id,user_id").createTempView("tt");
        //match titles
        ssc.sql(String.format("select org_id,user_id,concat_ws('_'," +
                "if(create_interview_times > %s,'面霸',null)," +
                "if(hire_times < %s,'养鸽人',null)," +
                "if(login_times > 150,'工具人',null)," +
                "if(19_times > 60,'达贡人',null)," +
                "if(send_offer_times > %s,'傲佛王子',null)," +
                "if(23_times > 30,'熬夜冠军',null)" +
                ") title from tt", createInterviewTimes, sendOfferTimes, sendOfferTimes))
                .createTempView("ttt");
        //final match title
        Dataset<Row> sql = ssc.sql("select u.org_id org_id,u.id user_id,u.created_at created_at,datediff('2021-12-02',u.created_at) period,my_udf(nvl(ttt.title,'养鸽人')) title " +
                " from dwd_users u left join ttt on u.org_id = ttt.org_id and u.id = ttt.user_id where u.created_at < '2021-12-02 00:00:00'");
        //sink
        Properties properties = new Properties();
        properties.put("user", "backstage_apps");
        properties.put("password", "p$!q+B0TE0");
        properties.put("batchsize", "10000");
        properties.put("isolationLevel", "NONE");
        properties.put("truncate", "true");
        sql.write().mode(SaveMode.Overwrite).jdbc("jdbc:mysql://rm-2zebe3h32729td79u.mysql.rds.aliyuncs.com:3306/backstage" +
                "?rewriteBatchedStatements=true" +
                "&useSSL=false", "moka_user_annual_title", properties);
        ssc.close();
    }

    @Test
    public void test() {
        System.out.println("".matches("\\s*"));
    }
}

/**
 * 6 称号1：面霸
 * 6 称号2：养鸽人
 * 3 称号3：工具人
 * 5 称号4：达贡人
 * 5 称号5：傲佛王子
 * 6 称号6：熬夜冠军
 */
@Slf4j
class MyUDF implements UDF1<String, String> {
    private Map<String, Integer> titles = new HashMap<>();
    private Random random = new Random();

    public MyUDF() {
        titles.put("面霸", 6);
        titles.put("养鸽人", 6);
        titles.put("工具人", 3);
        titles.put("达贡人", 5);
        titles.put("傲佛王子", 5);
        titles.put("熬夜冠军", 6);
    }

    @Override
    public String call(String in) {
        if ("养鸽人".equals(in) || in.isEmpty()) {
            in = "面霸_养鸽人_工具人_达贡人_傲佛王子_熬夜冠军";
        }
        List<Tuple2<Integer, String>> selfTitles = new ArrayList<>();
        int i = 0;
        for (String s : in.split("_")) {
            i += titles.get(s);
            selfTitles.add(Tuple2.of(i, s));
        }
        int nextInt = random.nextInt(i);
        for (Tuple2<Integer, String> tp : selfTitles) {
            if (nextInt < tp.f0) return tp.f1;
        }
        return "???";
    }
}

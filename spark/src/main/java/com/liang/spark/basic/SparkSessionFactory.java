package com.liang.spark.basic;

import com.liang.common.dto.Config;
import com.liang.common.util.ConfigUtils;
import com.liang.spark.udf.BitmapCount;
import com.liang.spark.udf.BitmapUnion;
import com.liang.spark.udf.CollectBitmap;
import com.liang.spark.udf.ToBitmap;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.RuntimeConfig;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

@Slf4j
@UtilityClass
public class SparkSessionFactory {
    public static SparkSession createSpark(String[] args) {
        String file = (args != null && args.length > 0) ? args[0] : null;
        initConfig(file);
        // 自定义JdbcDialect
        //JdbcDialects.unregisterDialect(JdbcDialects.get("jdbc:mysql"));
        //JdbcDialects.registerDialect(new FixedMySQLDialect());
        return initSpark();
    }

    private static void initConfig(String file) {
        Config config = ConfigUtils.createConfig(file);
        ConfigUtils.setConfig(config);
    }

    private static SparkSession initSpark() {
        SparkSession spark;
        try {
            spark = SparkSession
                    .builder()
                    .enableHiveSupport()
                    .getOrCreate();
        } catch (Exception e) {
            spark = SparkSession.builder()
                    .master("local[*]")
                    .getOrCreate();
        }
        RuntimeConfig conf = spark.conf();
        conf.set("spark.debug.maxToStringFields", "256");
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension");
        conf.set("spark.kryo.registrator", "org.apache.spark.HoodieSparkKryoRegistrar");
        // udf
        spark.udf().register("to_bitmap", new ToBitmap(), DataTypes.BinaryType);
        spark.udf().register("bitmap_count", new BitmapCount(), DataTypes.LongType);
        // udaf
        spark.udf().register("collect_bitmap", functions.udaf(new CollectBitmap(), Encoders.LONG()));
        spark.udf().register("bitmap_union", functions.udaf(new BitmapUnion(), Encoders.BINARY()));
        return spark;
    }
}

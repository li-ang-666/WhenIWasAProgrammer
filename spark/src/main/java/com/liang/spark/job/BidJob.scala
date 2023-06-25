package com.liang.spark.job

import com.liang.common.service.filesystem.ObsWriter
import org.apache.spark.api.java.function.ForeachPartitionFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

import java.util
import java.util.concurrent.TimeUnit


/**
 * create table test.bid_obs_tmp(js string)stored as textfile;
 */
object BidJob {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .config("spark.debug.maxToStringFields", "200")
      .enableHiveSupport().getOrCreate()
    val tableList: List[String] = List[String](
      "data_bid_dataplus_bid_class_df",
      "data_bid_dataplus_bid_notice_df",
      "data_bid_dataplus_bid_notice_ent_df",
      "data_bid_dataplus_bid_object_df",
      "data_bid_dataplus_bid_sections_df",
      "data_bid_dataplus_bid_win_df",
      "data_bid_dataplus_bid_win_ent_df",
      "data_bid_dataplus_bid_notice_content_df",
      "data_bid_dataplus_bid_win_content_df"
    )
    createView(spark, tableList)
    createUnionView(spark, tableList)
    createFinalView(spark)

    //hiveSink(spark, "obs_writer_test_hive")
    obsSink(spark)
    spark.stop()
  }

  private def createView(spark: SparkSession, tableList: List[String]): Unit = {
    import spark.implicits._
    tableList.foreach(tableName => {
      /*spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv("/Users/liang/Desktop/WhenIWasAProgrammer/spark/src/main/resources/tb.csv")*/
      spark.sql(s"select * from test.$tableName")
        .where("mid is not null and mid <> ''")
        .map(row => (row.getAs("mid").toString, row.json)).toDF("mid", "js")
        .groupBy(col("mid"))
        .agg(concat(lit("["), concat_ws(",", collect_list("js")), lit("]"))).toDF("mid", "js")
        .map(row => (s""""mid":"${row.getAs("mid").toString}"""", s""""${tableName}":${row.getAs("js").toString}""")).toDF("mid", "js")
        .createOrReplaceTempView(s"${tableName}_2")
    })
  }

  private def createUnionView(spark: SparkSession, tableList: List[String]): Unit = {
    val sql: String = tableList.map(tableName => s"select mid,js from ${tableName}_2").mkString("\n union all \n")
    spark.sql(sql)
      .createOrReplaceTempView("union_table")
  }

  private def createFinalView(spark: SparkSession): Unit = {
    spark.sql(
      """
        |
        |select concat('{', mid, ',', concat_ws(',',collect_list(js)), '}') js
        |from union_table
        |group by mid
        |
        |""".stripMargin)
      .persist(StorageLevel.DISK_ONLY)
      .createOrReplaceTempView("final_table")

  }

  private def obsSink(spark: SparkSession): Unit = {
    class Sink extends ForeachPartitionFunction[Row] {
      private var obsWriter: ObsWriter = _

      override def call(t: util.Iterator[Row]): Unit = {
        if (obsWriter == null) {
          obsWriter = new ObsWriter("obs://hadoop-obs/flink/tb1/")
          obsWriter.enableCache()
        }
        while (t.hasNext) {
          val row: Row = t.next()
          val content: String = row.getAs("js").toString
          obsWriter.update(content)
        }
        TimeUnit.SECONDS.sleep(10)
      }
    }
    spark.sql(
      """
        |select js from final_table
        |""".stripMargin)
      .repartition(1200)
      .foreachPartition(new Sink)
  }

  private def hiveSink(spark: SparkSession, tableName: String): Unit = {
    spark.sql(
      s"""
         |insert overwrite table test.${tableName}
         |select /*+ REPARTITION(1200) */ js from final_table
         |""".stripMargin)
  }
}

package com.liang.spark.job

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


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
    spark.sql(
      """
        |insert overwrite table test.bid_obs
        |select /*+ REPARTITION(5) */ concat('{', mid, ',', concat_ws(',',collect_list(js)), '}')
        |from unionTable
        |group by mid
        |""".stripMargin)
    spark.stop()
  }

  private def createView(spark: SparkSession, tableList: List[String]): Unit = {
    import spark.implicits._
    tableList.foreach(tableName => {
      //      spark.read.option("header", "true")
      //        .option("inferSchema", "true")
      //        .csv("/Users/liang/Desktop/WhenIWasAProgrammer/spark/src/main/resources/tb.csv")
      spark.sql(s"select * from test.$tableName")
        .where("mid is not null and mid <> 0")
        .map(row => (row.getAs("mid").toString, row.json)).toDF("mid", "js")
        .groupBy(col("mid"))
        .agg(concat(lit("["), concat_ws(",", collect_list("js")), lit("]"))).toDF("mid", "js")
        .map(row => (s""""mid":${row.getAs("mid").toString}""", s""""${tableName}":${row.getAs("js").toString}""")).toDF("mid", "js")
        .createOrReplaceTempView(tableName)
    })
  }

  private def createUnionView(spark: SparkSession, tableList: List[String]): Unit = {
    val sql: String = tableList.map(tableName => s"select * from ${tableName}").mkString(" union all ")
    spark.sql(sql)
      .createOrReplaceTempView("unionTable")
  }
}

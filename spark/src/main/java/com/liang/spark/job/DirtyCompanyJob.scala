package com.liang.spark.job

import com.liang.spark.basic.SparkSessionFactory
import org.apache.spark.sql.DataFrame

object DirtyCompanyJob {
  def main(args: Array[String]): Unit = {
    val spark = SparkSessionFactory.createSpark(args)
    spark.udf.register("get_sub_names", getSubNames _)
    spark.sql(
      """
        |select name,sub_name
        |from ods.ods_prism1_company_df t1
        |lateral view explode(split(get_sub_names(name),'#@#')) t2 as sub_name
        |where t1.pt='20230629' and t1.name is not null and t1.name <> ''
        |""".stripMargin)
      .createOrReplaceTempView("t1")

    val frame: DataFrame = spark.sql(
      """
        |select distinct dirty_company from test.test_dirty_company_df where dirty_company is not null and dirty_company <> ''
        |""".stripMargin)
    frame
      .createOrReplaceTempView("t2")

    spark.sql(
      """
        |insert overwrite table test.test_dirty_company_df_result_copy_new partition(pt='20230629')
        |select /*+ REPARTITION(600) */ distinct name from
        |t1 join t2 on t1.sub_name=t2.dirty_company
        |""".stripMargin)

  }

  private def getSubNames(name: String): String = {
    val reverseName: String = name.reverse
    val length: Int = Math.min(reverseName.length, 36)
    (1 to length).map(i => reverseName.substring(0, i).reverse).mkString("#@#")
  }
}

package com.liang.spark.dao

import org.apache.spark.sql.SparkSession

object BidDao {
  def getSql(spark: SparkSession, tableName: String): Unit = {
    spark.sql(s"select '${tableName}',mid,* from `${tableName}`")
      /*.map(row=>{
        (1 to row.length).map(cell=>row.json)
        ???
      })*/
  }
}

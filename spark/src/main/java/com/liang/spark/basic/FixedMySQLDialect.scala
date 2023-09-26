package com.liang.spark.basic

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types._

import java.sql.{SQLFeatureNotSupportedException, Types}
import java.util.Locale

class FixedMySQLDialect extends JdbcDialect {
  override def canHandle(url: String): Boolean = {
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:mysql")
  }

  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.VARBINARY && typeName.equals("BIT") && size != 1) {
      md.putLong("binarylong", 1)
      Option(LongType)
    } else if (sqlType == Types.BIT && typeName.equals("TINYINT")) {
      Option(BooleanType)
    } else if (sqlType == Types.DECIMAL) {
      Option(StringType)
    } else {
      None
    }
  }

  override def getTableExistsQuery(table: String): String = {
    s"SELECT 1 FROM $table LIMIT 1"
  }

  override def isCascadingTruncateTable(): Option[Boolean] = {
    Some(false)
  }

  override def getUpdateColumnTypeQuery(tableName: String, columnName: String, newDataType: String): String = {
    s"ALTER TABLE $tableName MODIFY COLUMN ${quoteIdentifier(columnName)} $newDataType"
  }

  override def getRenameColumnQuery(tableName: String, columnName: String, newName: String, dbMajorVersion: Int): String = {
    if (dbMajorVersion >= 8) {
      s"ALTER TABLE $tableName RENAME COLUMN ${quoteIdentifier(columnName)} TO" +
        s" ${quoteIdentifier(newName)}"
    } else {
      throw new SQLFeatureNotSupportedException(
        s"Rename column is only supported for MySQL version 8.0 and above.")
    }
  }

  override def quoteIdentifier(colName: String): String = {
    s"`$colName`"
  }

  override def getUpdateColumnNullabilityQuery(tableName: String, columnName: String, isNullable: Boolean): String = {
    throw new SQLFeatureNotSupportedException(s"UpdateColumnNullability is not supported")
  }

  override def getTableCommentQuery(table: String, comment: String): String = {
    s"ALTER TABLE $table COMMENT = '$comment'"
  }
}

package com.liang.spark.basic

import org.apache.spark.sql.jdbc.JdbcDialect
import org.apache.spark.sql.types._

import java.sql.{SQLFeatureNotSupportedException, Types}
import java.util.Locale

class FixedMySQLDialect extends JdbcDialect {

  override def canHandle(url: String): Boolean =
    url.toLowerCase(Locale.ROOT).startsWith("jdbc:mysql")

  override def getCatalystType(sqlType: Int, typeName: String, size: Int, md: MetadataBuilder): Option[DataType] = {
    if (sqlType == Types.VARBINARY && typeName.equals("BIT") && size != 1) {
      // This could instead be a BinaryType if we'd rather return bit-vectors of up to 64 bits as
      // byte arrays instead of longs.
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

  // See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
  override def getUpdateColumnTypeQuery(tableName: String, columnName: String, newDataType: String): String = {
    s"ALTER TABLE $tableName MODIFY COLUMN ${quoteIdentifier(columnName)} $newDataType"
  }

  // See Old Syntax: https://dev.mysql.com/doc/refman/5.6/en/alter-table.html
  // According to https://dev.mysql.com/worklog/task/?id=10761 old syntax works for
  // both versions of MySQL i.e. 5.x and 8.0
  // The old syntax requires us to have type definition. Since we do not have type
  // information, we throw the exception for old version.
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

  // See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
  // require to have column data type to change the column nullability
  // ALTER TABLE tbl_name MODIFY [COLUMN] col_name column_definition
  // column_definition:
  //    data_type [NOT NULL | NULL]
  // e.g. ALTER TABLE t1 MODIFY b INT NOT NULL;
  // We don't have column data type here, so throw Exception for now
  override def getUpdateColumnNullabilityQuery(tableName: String, columnName: String, isNullable: Boolean): String = {
    throw new SQLFeatureNotSupportedException(s"UpdateColumnNullability is not supported")
  }

  // See https://dev.mysql.com/doc/refman/8.0/en/alter-table.html
  override def getTableCommentQuery(table: String, comment: String): String = {
    s"ALTER TABLE $table COMMENT = '$comment'"
  }
}

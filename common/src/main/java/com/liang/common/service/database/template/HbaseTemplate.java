package com.liang.common.service.database.template;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.service.database.holder.HbaseConnectionHolder;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

/**
 * list                                                                 = show tables(所有namespace的)
 * list_namespace_tables "${namespace}"                                 = show tables(查看某个namespace下的)
 * create "${namespace}:${tableName}","${family}"                       = 建表
 * scan "${namespace}:${tableName}"                                     = select *
 * truncate "${namespace}:${tableName}"                                 = 清空
 * disable "${namespace}:${tableName}";drop "${namespace}:${tableName}" = 删表
 */
@Slf4j
public class HbaseTemplate {
    private final Connection connection;

    public HbaseTemplate(String name) {
        connection = HbaseConnectionHolder.getConnection(name);
    }

    public void upsert(HbaseOneRow hbaseOneRow) {
        try (Table table = connection.getTable(
                TableName.valueOf(hbaseOneRow.getNamespace(), hbaseOneRow.getTableName()))) {
            Put put = new Put(Bytes.toBytes(hbaseOneRow.getRowKey()));
            for (Map.Entry<String, Object> entry : hbaseOneRow.getColumnMap().entrySet()) {
                String[] familyAnfCol = entry.getKey().split(":");
                String family = familyAnfCol[0];
                String col = familyAnfCol[1];
                String value = String.valueOf(entry.getValue());
                put.addColumn(Bytes.toBytes(family),
                        Bytes.toBytes(col),
                        Bytes.toBytes(value));
                table.put(put);
            }
        } catch (Exception e) {
            log.warn("hbase upsert error: {}", hbaseOneRow);
        }
    }

    public void delete(HbaseOneRow hbaseOneRow) {
        try (Table table = connection.getTable(
                TableName.valueOf(hbaseOneRow.getNamespace(), hbaseOneRow.getTableName()))) {
            Delete delete = new Delete(Bytes.toBytes(hbaseOneRow.getRowKey()));
            table.delete(delete);
        } catch (Exception e) {
            log.warn("hbase delete error: {}", hbaseOneRow);
        }
    }
}

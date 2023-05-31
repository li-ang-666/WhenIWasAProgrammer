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
 * <p>列出所有表:
 * <p>list
 * <p>列出某个namespace下的表:
 * <p>list_namespace_tables ${namespace}"'
 * <p>建表:
 * <p>create '${namespace}:${tableName}','${family}'
 * <p>查看所有row:
 * <p>scan '${namespace}:${tableName}',{FORMATTER => 'toString'}
 * <p>查看某个row:
 * <p>get '${namespace}:${tableName}','${rowKey}',{FORMATTER => 'toString'}
 * <p>清表:
 * <p>truncate '${namespace}:${tableName}'
 * <p>删表:
 * <p>disable '${namespace}:${tableName}';drop '${namespace}:${tableName}'
 */
@Slf4j
public class HbaseTemplate {
    private final String name;
    private final Connection connection;

    public HbaseTemplate(String name) {
        this.name = name;
        connection = HbaseConnectionHolder.getConnection(name);
    }

    private void preExecute(String action, HbaseOneRow hbaseOneRow) {
        log.debug("hbaseTemplate {}: {}, {}", action, name, hbaseOneRow);
    }

    private void whenError(String action, HbaseOneRow hbaseOneRow, Exception e) {
        log.error("hbaseTemplate {} error: {}, {}", action, name, hbaseOneRow, e);
    }

    public void upsert(HbaseOneRow hbaseOneRow) {
        preExecute("upsert", hbaseOneRow);
        Map<String, Object> columnMap = hbaseOneRow.getColumnMap();
        if (columnMap.isEmpty()) {
            return;
        }
        try (Table table = connection.getTable(
                TableName.valueOf(hbaseOneRow.getSchema().getNamespace(), hbaseOneRow.getSchema().getTableName()))) {
            Put put = new Put(Bytes.toBytes(hbaseOneRow.getRowKey()));
            for (Map.Entry<String, Object> entry : columnMap.entrySet()) {
                String[] familyAndCol = entry.getKey().split(":");
                String family = familyAndCol[0];
                String col = familyAndCol[1];
                String value = String.valueOf(entry.getValue());
                put.addColumn(Bytes.toBytes(family),
                        Bytes.toBytes(col),
                        Bytes.toBytes(value));
            }
            table.put(put);
        } catch (Exception e) {
            whenError("upsert", hbaseOneRow, e);
        }
    }

    public void deleteRow(HbaseOneRow hbaseOneRow) {
        preExecute("deleteRow", hbaseOneRow);
        try (Table table = connection.getTable(
                TableName.valueOf(
                        hbaseOneRow.getSchema().getNamespace(),
                        hbaseOneRow.getSchema().getTableName()))) {
            Delete delete = new Delete(Bytes.toBytes(hbaseOneRow.getRowKey()));
            table.delete(delete);
        } catch (Exception e) {
            whenError("deleteRow", hbaseOneRow, e);
        }
    }
}

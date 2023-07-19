package com.liang.common.service.database.template;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.AbstractCache;
import com.liang.common.service.Logging;
import com.liang.common.service.database.holder.HbaseConnectionHolder;
import lombok.Synchronized;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 列出所有表:
 * list
 * <p>
 * 列出某个namespace下的表:
 * list_namespace_tables ${namespace}"'
 * <p>
 * 建表:
 * create '${namespace}:${tableName}','${family}'
 * <p>
 * 查看所有row:
 * scan '${namespace}:${tableName}',{FORMATTER => 'toString'}
 * <p>
 * 查看某个row:
 * get '${namespace}:${tableName}','${rowKey}',{FORMATTER => 'toString'}
 * <p>
 * 清表:
 * truncate '${namespace}:${tableName}'
 * <p>
 * 删表:
 * disable '${namespace}:${tableName}';drop '${namespace}:${tableName}'
 */
@Slf4j
public class HbaseTemplate extends AbstractCache<HbaseSchema, HbaseOneRow> {
    private final static int DEFAULT_CACHE_MILLISECONDS = 500;
    private final static int DEFAULT_CACHE_RECORDS = 1024;
    private final Connection pool;
    private final Logging logging;

    public HbaseTemplate(String name) {
        super(DEFAULT_CACHE_MILLISECONDS, DEFAULT_CACHE_RECORDS, HbaseOneRow::getSchema);
        pool = new HbaseConnectionHolder().getPool(name);
        logging = new Logging(this.getClass().getSimpleName(), name);
    }

    @Override
    @Synchronized
    protected void updateImmediately(HbaseSchema schema, List<HbaseOneRow> hbaseOneRows) {
        logging.beforeExecute();
        try (Table table = getTable(schema)) {
            List<Put> puts = new ArrayList<>();
            for (HbaseOneRow hbaseOneRow : hbaseOneRows) {
                Put put = new Put(Bytes.toBytes(hbaseOneRow.getRowKey()));
                for (Map.Entry<String, Object> entry : hbaseOneRow.getColumnMap().entrySet()) {
                    String col = entry.getKey();
                    String value = String.valueOf(entry.getValue());
                    byte[] valueArr = value.equalsIgnoreCase("null") ? null : Bytes.toBytes(value);
                    put.addColumn(Bytes.toBytes(hbaseOneRow.getSchema().getColumnFamily()),
                            Bytes.toBytes(col), valueArr);
                }
                puts.add(put);
            }
            // todo: 源码调用了table.batch()方法,好像可以同时包含put和delete
            table.put(puts);
            Object methodArg = hbaseOneRows.size() > 100 ? hbaseOneRows.size() + "条" : hbaseOneRows;
            logging.afterExecute("upsert", methodArg);
        } catch (Exception e) {
            logging.ifError("upsert", hbaseOneRows, e);
        }
    }

    public HbaseOneRow getRow(HbaseOneRow hbaseOneRow) {
        logging.beforeExecute();
        HbaseSchema schema = hbaseOneRow.getSchema();
        String rowKey = hbaseOneRow.getRowKey();
        HbaseOneRow resultOneRow = new HbaseOneRow(schema, rowKey);
        try (Table table = getTable(schema)) {
            Get get = new Get(Bytes.toBytes(rowKey));
            // 每个result是一行
            Result result = table.get(get);
            for (Cell cell : result.listCells()) {
                String columnFamily = Bytes.toString(CellUtil.cloneFamily(cell));
                if (columnFamily.equals(schema.getColumnFamily())) {
                    resultOneRow.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
            logging.afterExecute("getRow", hbaseOneRow);
            return resultOneRow;
        } catch (Exception e) {
            logging.ifError("getRow", hbaseOneRow, e);
            return resultOneRow;
        }
    }

    private Table getTable(HbaseSchema schema) throws Exception {
        return pool.getTable(TableName.valueOf(
                schema.getNamespace(),
                schema.getTableName()));
    }
}

package com.liang.common.service.database.template;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.database.holder.HbaseConnectionHolder;
import com.liang.common.util.DateTimeUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;
import java.util.concurrent.TimeUnit;

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
    private final Connection pool;
    private final TemplateLogger logger;
    private final Map<Tuple2<HbaseSchema, OptType>, List<HbaseOneRow>> cache = new HashMap<>();

    public HbaseTemplate(String name) {
        pool = new HbaseConnectionHolder().getPool(name);
        logger = new TemplateLogger(this.getClass().getSimpleName(), name);
        new Thread(new Sender(this)).start();
    }

    public void upsert(HbaseOneRow... hbaseOneRows) {
        upsert(Arrays.asList(hbaseOneRows));
    }

    public void upsert(List<HbaseOneRow> hbaseOneRows) {
        for (HbaseOneRow hbaseOneRow : hbaseOneRows) {
            synchronized (cache) {
                Tuple2<HbaseSchema, OptType> key = Tuple2.of(hbaseOneRow.getSchema(), OptType.UPSERT);
                cache.putIfAbsent(key, new ArrayList<>());
                cache.get(key).add(hbaseOneRow);
            }
        }
    }

    private void upsert(HbaseSchema schema, List<HbaseOneRow> hbaseOneRows) {
        logger.beforeExecute("upsert", hbaseOneRows);
        try (Table table = getTable(schema)) {
            ArrayList<Put> puts = new ArrayList<>();
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
            table.put(puts);
        } catch (Exception e) {
            logger.ifError("upsert", hbaseOneRows, e);
        }
        logger.afterExecute("upsert", hbaseOneRows);
    }

    public List<Tuple4<String, String, String, String>> getRow(HbaseOneRow hbaseOneRow) {
        logger.beforeExecute("getRow", hbaseOneRow);
        List<Tuple4<String, String, String, String>> resultList = new ArrayList<>();
        try (Table table = getTable(hbaseOneRow.getSchema())) {
            Get get = new Get(Bytes.toBytes(hbaseOneRow.getRowKey()));
            Result result = table.get(get);
            for (Cell cell : result.listCells()) {
                resultList.add(Tuple4.of(
                        Bytes.toString(CellUtil.cloneFamily(cell)),
                        Bytes.toString(CellUtil.cloneQualifier(cell)),
                        Bytes.toString(CellUtil.cloneValue(cell)),
                        DateTimeUtils.fromUnixTime(cell.getTimestamp() / 1000, "yyyy-MM-dd HH:mm:ss")
                ));
            }
        } catch (Exception e) {
            logger.ifError("getRow", hbaseOneRow, e);
        }
        logger.afterExecute("getRow", hbaseOneRow);
        return resultList;
    }

    private Table getTable(HbaseSchema schema) throws Exception {
        return pool.getTable(
                TableName.valueOf(
                        schema.getNamespace(),
                        schema.getTableName()));
    }

    private static class Sender implements Runnable {
        private final HbaseTemplate hbaseTemplate;

        public Sender(HbaseTemplate hbaseTemplate) {
            this.hbaseTemplate = hbaseTemplate;
        }

        @Override
        @SneakyThrows
        public void run() {
            while (true) {
                TimeUnit.MILLISECONDS.sleep(1000);
                Map<Tuple2<HbaseSchema, OptType>, List<HbaseOneRow>> copyCache;
                synchronized (hbaseTemplate.cache) {
                    copyCache = new HashMap<>(hbaseTemplate.cache);
                    hbaseTemplate.cache.clear();
                }
                for (Map.Entry<Tuple2<HbaseSchema, OptType>, List<HbaseOneRow>> entry : copyCache.entrySet()) {
                    hbaseTemplate.upsert(entry.getKey().f0, entry.getValue());
                }
            }
        }
    }

    private enum OptType {
        UPSERT, DELETE
    }
}

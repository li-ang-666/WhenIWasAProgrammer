package com.liang.common.service.database.template;

import com.liang.common.dto.HbaseOneRow;
import com.liang.common.dto.HbaseSchema;
import com.liang.common.service.database.holder.HbaseConnectionHolder;
import com.liang.common.service.database.template.inner.TemplateLogger;
import com.liang.common.util.DateTimeUtils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
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
    private final static int DEFAULT_CACHE_TIME = 500;
    private final static int DEFAULT_CACHE_SIZE = 1024;
    private final Connection pool;
    private final TemplateLogger logger;
    private final Map<HbaseSchema, List<HbaseOneRow>> cache = new HashMap<>();

    public HbaseTemplate(String name) {
        this(name, DEFAULT_CACHE_TIME);
    }

    public HbaseTemplate(String name, int cacheTime) {
        pool = new HbaseConnectionHolder().getPool(name);
        logger = new TemplateLogger(this.getClass().getSimpleName(), name);
        new Thread(new Sender(this, cacheTime)).start();
    }

    public void upsert(HbaseOneRow... hbaseOneRows) {
        if (hbaseOneRows == null || hbaseOneRows.length == 0) {
            return;
        }
        upsert(Arrays.asList(hbaseOneRows));
    }

    public void upsert(List<HbaseOneRow> hbaseOneRows) {
        if (hbaseOneRows == null || hbaseOneRows.isEmpty()) {
            return;
        }
        for (HbaseOneRow hbaseOneRow : hbaseOneRows) {
            synchronized (cache) {
                HbaseSchema key = hbaseOneRow.getSchema();
                cache.putIfAbsent(key, new ArrayList<>());
                List<HbaseOneRow> list = cache.get(key);
                list.add(hbaseOneRow);
                if (list.size() >= DEFAULT_CACHE_SIZE) {
                    upsert(key, list);
                    cache.remove(key);
                }
            }
        }
    }

    private synchronized void upsert(HbaseSchema schema, List<HbaseOneRow> hbaseOneRows) {
        if (hbaseOneRows == null || hbaseOneRows.isEmpty()) {
            return;
        }
        logger.beforeExecute();
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
            logger.afterExecute("upsert", hbaseOneRows);
        } catch (Exception e) {
            logger.ifError("upsert", hbaseOneRows, e);
        }
    }

    public List<Tuple4<String, String, String, String>> getRow(HbaseOneRow hbaseOneRow) {
        logger.beforeExecute();
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
            logger.afterExecute("getRow", hbaseOneRow);
            return resultList;
        } catch (Exception e) {
            logger.ifError("getRow", hbaseOneRow, e);
            return resultList;
        }
    }

    private Table getTable(HbaseSchema schema) throws Exception {
        return pool.getTable(TableName.valueOf(
                schema.getNamespace(),
                schema.getTableName()));
    }

    private static class Sender implements Runnable {
        private final HbaseTemplate hbaseTemplate;
        private final int cacheTime;

        public Sender(HbaseTemplate hbaseTemplate, int cacheTime) {
            this.hbaseTemplate = hbaseTemplate;
            this.cacheTime = cacheTime;
        }

        @Override
        @SneakyThrows
        @SuppressWarnings("InfiniteLoopStatement")
        public void run() {
            while (true) {
                TimeUnit.MILLISECONDS.sleep(cacheTime);
                if (hbaseTemplate.cache.isEmpty()) {
                    continue;
                }
                Map<HbaseSchema, List<HbaseOneRow>> copyUpsertCache;
                synchronized (hbaseTemplate.cache) {
                    if (hbaseTemplate.cache.isEmpty()) {
                        continue;
                    }
                    copyUpsertCache = new HashMap<>(hbaseTemplate.cache);
                    hbaseTemplate.cache.clear();
                }
                for (Map.Entry<HbaseSchema, List<HbaseOneRow>> entry : copyUpsertCache.entrySet()) {
                    hbaseTemplate.upsert(entry.getKey(), entry.getValue());
                }
            }
        }
    }
}

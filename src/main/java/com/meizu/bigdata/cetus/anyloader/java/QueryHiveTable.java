package com.meizu.bigdata.cetus.anyloader.java;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hive.jdbc.HiveStatement;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class QueryHiveTable implements Runnable{
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(QueryHiveTable.class);
    Connection connection;
    BlockingQueue<Map<String, String>[]>[] msgQueue;
    int parrel;
    int batchSize;
    String sql;

    public QueryHiveTable(Connection connection, BlockingQueue<Map<String, String>[]>[] msgQueue, int parrel, int batchSize, String sql) {
        this.connection = connection;
        this.msgQueue = msgQueue;
        this.parrel = parrel;
        this.batchSize = batchSize;
        this.sql = sql;
    }

    public QueryHiveTable(Connection connection) {
        this.connection = connection;
    }

    private int currentQueueIndex = 0;
    private BlockingQueue<Map<String, String>[]> rollBlockingQueue() {
        if (currentQueueIndex < parrel) {
            return msgQueue[currentQueueIndex++];
        } else {
            currentQueueIndex = 0;
            return msgQueue[currentQueueIndex++];
        }
    }


    @Override
    public void run() {
        StopWatch watch = new StopWatch();
        watch.start();

        ArrayList<Map<String,String>> msgBuffer = new ArrayList<>();
        try (HiveStatement statement = (HiveStatement) connection.createStatement();
             ResultSet rs = statement.executeQuery(sql);
        ) {
            ResultSetMetaData rmd = rs.getMetaData();
            int count = rmd.getColumnCount();
            if (count == 0) {
                throw new NullPointerException("没有指定查询列");
            }

            while (rs.next()) {
                Map<String, String> row = new HashMap<>();
                msgBuffer.add(row);
                for (int j = 1; j <= count; j++) {
                    row.put(rmd.getColumnName(j), rs.getString(j));
                }

                if (msgBuffer.size() == batchSize) {
                    Map<String, String>[] tmpArrayBuffer = msgBuffer.toArray(new HashMap[batchSize]);
                    rollBlockingQueue().put(tmpArrayBuffer);
                    msgBuffer.clear();
                }
            }

            if (msgBuffer.size() != 0) {
                Map<String, String>[] tmpArrayBuffer = msgBuffer.toArray(new HashMap[msgBuffer.size()]);
                rollBlockingQueue().put(tmpArrayBuffer);
                msgBuffer.clear();
            }

            watch.stop();
            log.info("处理 sql=>" + sql + " 耗时 " + watch.getTime() + " ms");
        } catch (SQLException | InterruptedException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void execute(String sql) {
        try (HiveStatement statement = (HiveStatement) connection.createStatement();
        ) {
            log.info("exec sql => {}", sql);
            boolean success = statement.execute(sql);
        } catch (SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}

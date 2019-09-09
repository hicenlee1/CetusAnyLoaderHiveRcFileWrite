package com.meizu.bigdata.cetus.anyloader.java;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class LocationSource {
    String dbName;
    String tableName;
    String statDate;
    String statHour;
    String statMinute;
    Connection connection;
    ExecutorService readService;
    private static final Logger log = LoggerFactory.getLogger(LocationSource.class);

    public LocationSource(Connection connection, String dbName, String tableName, String statDate,
                          String statHour, String statMinute) {
        this.connection = connection;
        this.dbName = dbName;
        this.tableName = tableName;
        this.statDate = statDate;
        this.statHour = statHour;
        this.statMinute = statMinute;
        readService = Executors.newFixedThreadPool(1);
    }

    public Future<?> readLocation(final BlockingQueue<Map<String, String>[]>[] msgQueue,
                               int parrel, int batchSize) {
        String sqlFormat = "select imei,value_str,url from %s where stat_date=%s and stat_hour=%s and stat_minute=%s";
        String sql = String.format(sqlFormat, dbName + ".wy_ods_poi_location_result_minute_json", statDate, statHour, statMinute);
        log.info("sql=>" + sql);
        return readService.submit(new QueryHiveTable(connection, msgQueue, parrel, batchSize, sql));
    }

    public void shutDown() {
        readService.shutdown();
    }
}

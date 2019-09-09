package com.meizu.bigdata.cetus.anyloader.java;

import com.meizu.bigdata.cetus.anyloader.java.model.Model;
import com.meizu.bigdata.cetus.anyloader.java.writer.HiveWriterClient;
import com.meizu.bigdata.hadoop.config.HdfsConfigurationFactory;
import com.meizu.bigdata.hadoop.config.HiveConnectionFactory;
import com.meizu.bigdata.hadoop.config.Key;
import com.meizu.bigdata.hadoop.config.auth.DefaultPasswordCallback;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.File;
import java.sql.Connection;
import java.text.MessageFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Future;

public class GetResponse {
    private static final Logger log = LoggerFactory.getLogger(GetResponse.class);
    public static boolean READ_FINISH = false;
    public static boolean RUNTIME_ERROR = false;
    public static boolean PARSE_FINISH = false;

    public static void main(String[] args) {
        new GetResponse().start(args);
        //一定要System.exit 因为引入的security-common包中有线程循环检测kerberos ticket,不退出JVM不会结束
        System.exit(0);
    }

    public void start(String[] args) {
                if (args.length < 10) {
            System.out.println("缺少参数");
            System.exit(1);
        }

        final String hadoopUserName = args[0];
//        final String dbName = "ext_metis";
        final String dbName = args[1];
//        final String tableName = "wy_json_response";
        final String tableName = args[2];
        final String statDate = args[3];
        final String statHour = args[4];
        final String statMinute = args[5];

        int parrel = Integer.parseInt(args[6]);
        int delay = Integer.parseInt(args[7]);
        int hdfsFileSize = Integer.parseInt(args[8]);
        boolean algoCluster = Boolean.parseBoolean(args[9]);
        //String hiveBase = args[10];
        String codec = "com.hadoop.compression.lzo.LzoCodec";
        int batchSize = 100;

        BlockingQueue<Map<String,String>[]>[] msgQueue = new ArrayBlockingQueue[parrel];
        for (int i = 0; i < parrel; i++) {
            msgQueue[i] = new ArrayBlockingQueue(1);
        }

        BlockingQueue<Map<String, String>[]>[] writeMsgQueue = new ArrayBlockingQueue[hdfsFileSize];
        for (int i = 0; i < hdfsFileSize; i++) {
            writeMsgQueue[i] = new ArrayBlockingQueue<>(2);
        }

        ApplicationContext ac = new ClassPathXmlApplicationContext("classpath:/spring-service.xml");

        setHadoopEnv(hadoopUserName);
        HdfsConfigurationFactory factory = (HdfsConfigurationFactory) ac.getBean("hdfsConfigurationFactory");

        Configuration config = null;
        try {
             config = getConfig(factory, hadoopUserName);
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            log.info("task error<<<<");
            System.exit(1);
        }

        HiveConnectionFactory hcf = getHiveConnectionFactory(dbName);
        try(Connection conn = hcf.newConnection(hadoopUserName,new DefaultPasswordCallback("aquarius", "1a60ef13f72adfc6fe719370448df3b8"));) {
//            System.out.println("====================> " + conn);
            LocationSource source = new LocationSource(conn, dbName, tableName, statDate, statHour, statMinute);

            Future<?> readFuture = source.readLocation(msgQueue, parrel, batchSize);

            ParseLocation parse = new ParseLocation(parrel, delay, msgQueue, writeMsgQueue);
            parse.parseLocation();

            String path = getPath(dbName, tableName, statHour, statDate, statMinute);
            String[] columns = new String[]{"imei", "value_str", "json_response"};
            HiveWriterClient client = new HiveWriterClient(algoCluster, hdfsFileSize, path, columns, config, writeMsgQueue, codec);
            client.writeRcFile();

            readFuture.get();
            log.info("read one");
            READ_FINISH = true;
            source.shutDown();

            parse.shutdown();
            PARSE_FINISH = true;
            log.info("parse done");


            client.shutdown();
            log.info("hdfs file write done");
            //fix  hive partiton
            new QueryHiveTable(conn).execute(addPartitionSql(dbName, tableName, statHour, statDate, statMinute));
            log.info("add partiton done");

            log.info("task done");


        } catch (Exception e) {
            RUNTIME_ERROR = true;
            log.error(e.getMessage(), e);

            log.error("task error");
            System.exit(1);
        }
    }

    private void setHadoopEnv(String hadoopUserName) {
        System.setProperty("hadoop.home.dir", new File(".").getAbsolutePath());
        System.setProperty("HADOOP_USER_NAME", hadoopUserName);
    }

    private Configuration getConfig(HdfsConfigurationFactory factory, String hadoopUserName) {
        DefaultPasswordCallback pwdCallback = new DefaultPasswordCallback();
        pwdCallback.setCode("cetus");
        final String key = LibraYardConfig.getInstance().get("cetus.libra.security.key");
        pwdCallback.setKey(key);

        return factory.newConfiguration(hadoopUserName, pwdCallback);
    }

    private HiveConnectionFactory getHiveConnectionFactory(final String dbName) {
        HadoopComonYardConfig hadoopConfig = HadoopComonYardConfig.getInstance();
        String hiveUrl = hadoopConfig.get(Key.HIVE_URL);
        hiveUrl = MessageFormat.format(hiveUrl, dbName);
        log.info("hive url:" + hiveUrl);

        HiveConnectionFactory hcf = new HiveConnectionFactory();
        //hcf.setHiveUrl("jdbc:hive2://yjd-dn-52-110.meizu.mz:2181,yjd-dn-52-111.meizu.mz:2181,yjd-dn-52-112.meizu.mz:2181/{0};serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2");
        hcf.setHiveUrl(hiveUrl);
        hcf.setHadoopSecurityAuthorization("Y");
        return hcf;
    }

    private String getPath(String dbName, String tableName, String part1, String part2, String part3) {
        return dbName + ".db" + "/" + tableName + "/stat_hour=" + part1 + "/stat_date=" + part2 + "/stat_minute=" + part3;
    }

    private String addPartitionSql(String dbName, String tableName, String part1, String part2, String part3) {
        String sql = "alter table %s.%s add if not exists partition(stat_hour='%s',stat_date=%s,stat_minute='%s')";
        return String.format(sql, dbName, tableName, part1, part2, part3);
    }
}

package com.meizu.bigdata.cetus.anyloader.java.writer;

import com.google.common.collect.Lists;
import com.meizu.bigdata.cetus.anyloader.java.GetResponse;
import com.meizu.bigdata.cetus.anyloader.java.HadoopComonYardConfig;
import com.meizu.bigdata.cetus.anyloader.java.model.DataPath;
import jline.internal.Log;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class HiveWriterClient {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(HiveWriterClient.class);

    private String hiveBasePath;

    private String path;

    private String fullPath;

    //hive 文件个数
    private int destFileSize;

    private String[] fileSplits;
    private String[] columns;

    private String codec;

    BlockingQueue<Map<String, String>[]>[] writeMessages;
    Configuration conf;
    ExecutorService executor;
    CountDownLatch latch;
    CompletionService<Boolean> cs;
    List<Future<Boolean>> futureList;


    public HiveWriterClient(final boolean algoCluster, int destFileSize, String path, String[] columns, Configuration conf,
                            BlockingQueue<Map<String, String>[]>[] writeMessages, String codec) {
        hiveBasePath = StringUtils.defaultIfBlank(HadoopComonYardConfig.getInstance().get("hive.warehouse"), "/user/hive/warehouse/");
        if (!hiveBasePath.endsWith("/")) {
            hiveBasePath = hiveBasePath + "/";
        }
        String prefix = algoCluster ? "hdfs://mlcluster" : "";
        fullPath = prefix + hiveBasePath + path;

        if (!fullPath.endsWith("/")) {
            fullPath = fullPath + "/";
        }
        this.destFileSize = destFileSize;
        this.columns = columns;
        this.conf = conf;
        this.writeMessages = writeMessages;
        this.codec = codec;
        log.info("url==>" + fullPath);
    }

    public List<Future<Boolean>> writeRcFile() {

        executor = Executors.newFixedThreadPool(destFileSize, new HiveWriteThreadFacotry("hive-writer"));
        cs = new ExecutorCompletionService<>(executor);
        futureList = new ArrayList<>(destFileSize);
        final AtomicLong count = new AtomicLong();
        //latch = new CountDownLatch(destFileSize);
        log.info("开始写入HIVE-HDFS数据");
        for (int i = 0; i < destFileSize; i++) {
            DataPath tmpPath = new DataPath(fullPath, i);
            final int index = i;
            Future<Boolean> f = cs.submit(
                    new Callable<Boolean>() {
                        @Override
                        public Boolean call() {
                            boolean success = false;
                            RcFileWriter rcFileWriter = new RcFileWriter(conf, new DataPath(fullPath, index), codec, destFileSize, columns,
                                    writeMessages[index], count);
                            try {
                                rcFileWriter.initConf();
                                rcFileWriter.initWriter();
                                rcFileWriter.write();
                                success = true;
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            } finally {
                                //latch.countDown();
                                if (rcFileWriter != null) {
                                    rcFileWriter.destroyWrite();
                                }
                                return success;
                            }
                        }
                    }
            );
            futureList.add(f);
        }
        return futureList;

    }

    public void shutdown() {
        try {
            for (int i = 0; i < destFileSize; i++) {
                boolean success = cs.take().get();
                log.info("hive write thread " + i + "运行结束");
                if (!success) {
                    GetResponse.RUNTIME_ERROR = true;
                    break;
                }
            }
        } catch (Exception e) {
            log.error(e.getMessage(), e);
        } finally {
            for (Future<Boolean> f : futureList) {
                log.info("关闭hive-write线程");
                f.cancel(true);
            }
        }
        executor.shutdown();
//        try {
//            latch.await();
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        } finally {
//            executor.shutdown();
//        }
        log.info("所有writer写入HIVE-HDFS数据结束");
    }

}

class HiveWriteThreadFacotry implements ThreadFactory {

    private String prefix;

    AtomicInteger index = new AtomicInteger(0);

    public HiveWriteThreadFacotry(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        String name = prefix + "-thread" + index.getAndIncrement();
        return new Thread(r, name);
    }
}

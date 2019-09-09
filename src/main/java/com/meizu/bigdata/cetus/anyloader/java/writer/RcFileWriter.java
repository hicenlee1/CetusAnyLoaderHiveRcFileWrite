package com.meizu.bigdata.cetus.anyloader.java.writer;

import com.meizu.bigdata.cetus.anyloader.java.DxFileUtils;
import com.meizu.bigdata.cetus.anyloader.java.GetResponse;
import com.meizu.bigdata.cetus.anyloader.java.HadoopComonYardConfig;
import com.meizu.bigdata.cetus.anyloader.java.model.DataPath;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.RCFile;
import org.apache.hadoop.hive.ql.io.RCFileOutputFormat;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

public class RcFileWriter implements HiveWriter{
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(RcFileWriter.class);


    private Configuration conf;
    //.gz   .lzo  .lzop  .bz2
    private String codec;
    //hive 文件个数
    private int fileSize;

    private DataPath dataPath;
    private static boolean cleanHdfsPathFinished = false;

    FileSystem fileSystem;
    Path hdfsPath;

    String[] columns;

    BlockingQueue<Map<String, String>[]> writeMsgs;

    RCFile.Writer rcFileWriter = null;
    AtomicLong count;
    public RcFileWriter(Configuration conf, DataPath dataPath, String codec, int fileSize, String[] columns,
                        BlockingQueue<Map<String, String>[]> writeMsgs, AtomicLong count) {
        this.conf = conf;
        this.dataPath = dataPath;
        this.codec = codec;
        if (fileSize <= 0) {
            fileSize = 1;
        }
        this.fileSize = fileSize;
        this.columns = columns;
        this.writeMsgs = writeMsgs;
        this.count = count;
    }



    private void clearPath() {
        lock.lock();
        try {
            if (!cleanHdfsPathFinished) {
                Path path = new Path(dataPath.getPath());
                try {
                    FileSystem fs = FileSystem.get(path.toUri(), conf);
                    if (fs.exists(path)) {
                        FileStatus[] fileList = fs.listStatus(path);
                        for (int i = 0; i < fileList.length; i++) {
                            Path p = fileList[i].getPath();
                            FileStatus fileStatus = fs.getFileStatus(p);
                            if (!fileStatus.isDirectory()) {
                                fs.delete(p, false);
                                log.info(String.format("FILE DELETE: %s", p));
                            } else {
                                log.info(String.format("SKIP DELETE: %s", p));
                            }
                        }
                    } else {
                        fs.mkdirs(path);
                    }
                    cleanHdfsPathFinished = true;
                } catch (IOException e) {
                    e.printStackTrace();
                    throw new RuntimeException(e);
                }
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void initConf() {

        clearPath();
        dataPath.setPath(DxFileUtils.parseFileSequence(dataPath));

        if (!StringUtils.isBlank(codec) && codec.contains("LzoCodec")) {
            String p = dataPath.getPath();
            if (!p.endsWith(".lzo"))
                dataPath.setPath(p + ".lzo");
        }

    }

    @Override
    public void initWriter() {
        try {
            hdfsPath = new Path((dataPath.getPath()));
            fileSystem = FileSystem.get(hdfsPath.toUri(), conf);
            RCFileOutputFormat.setColumnNumber(conf, columns.length);
            CompressionCodec tmpCodec = (CompressionCodec) Class.forName(this.codec).newInstance();
            rcFileWriter = new RCFile.Writer(fileSystem, conf, hdfsPath, null, new SequenceFile.Metadata(), tmpCodec);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write() {
        try {
            while (!GetResponse.RUNTIME_ERROR) {
                Map<String, String>[] msgs = writeMsgs.poll(1, TimeUnit.SECONDS);
                if (msgs == null) {
                    if (GetResponse.PARSE_FINISH) {
                        break;
                    } else {
                        continue;
                    }
                }
                for (Map<String,String> msg : msgs) {
                    BytesRefArrayWritable row = new BytesRefArrayWritable();
                    for (int i = 0; i < columns.length; i++) {
                        byte[] bytes = null;
                        BytesRefWritable column = new BytesRefWritable();
                        if (msg.get(columns[i]) == null) {
                            bytes = new byte[0];
                        } else {
                            bytes = msg.get(columns[i]).getBytes("UTF-8");
                        }
                        column.set(bytes, 0, bytes.length);
                        row.set(i, column);
                    }
                    rcFileWriter.append(row);
                    count.getAndIncrement();
                }
                if (count.get() % 10000 == 0) {
                    log.info("hdfs 写入" + count.get() + "条数据");
                }
            }
            log.info("hdfs 写入" + count.get() + "条数据");
        } catch (InterruptedException e) {
          log.error("interrupt " + Thread.currentThread().getName()+", returning");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void destroyWrite() {
        if (rcFileWriter != null) {
            try {
                log.info("开始关闭rcfilewriter");
                rcFileWriter.close();
                log.info("关闭rcfilewriter结束");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}

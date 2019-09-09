package com.meizu.bigdata.cetus.anyloader.java;

import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.SocketTimeoutException;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;

class ParseLocationRunnable implements Callable<Boolean> {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(ParseLocationRunnable.class);
    BlockingQueue<Map<String, String>[]> msgQueue;

    BlockingQueue<Map<String, String>[]>[] writeMsgQueues;
    BlockingQueue<Map<String, String>[]> writeMsgQueue;
    int delay;

    //LongAdder longAdder;
    AtomicLong longAdder;

    CountDownLatch latch;

    public ParseLocationRunnable(BlockingQueue<Map<String, String>[]> msgQueue, int delay, AtomicLong longAdder,
                                 CountDownLatch latch, BlockingQueue<Map<String,String>[]>[] writeMsgQueues, int sequence) {
        this.msgQueue = msgQueue;
        this.delay = delay;
        this.longAdder = longAdder;
        this.latch = latch;
        this.writeMsgQueues = writeMsgQueues;
        writeMsgQueue = writeMsgQueues[sequence % writeMsgQueues.length];
    }

    @Override
    public Boolean call() {
        try {
            Map<String, String>[] msgs = null;
            while (!GetResponse.RUNTIME_ERROR) {
                try {
                    msgs = msgQueue.poll(200, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (msgs == null) {
                    if (GetResponse.READ_FINISH == true) {
                        break;
                    } else {
                        continue;
                    }
                }
                setLocation(msgs);
                writeMsgQueue.put(msgs);
            }

            return true;
        } catch (Exception e) {
            log.info(e.getMessage(), e);
            return false;
        } finally {
            latch.countDown();
        }
    }
    public void setLocation(Map<String, String>[] msgs) {
        if (msgs != null) {
            for (Map<String, String> map : msgs) {
                String locationResponse = callForLocation(map.get("url"), map.get("value_str"), delay);
                map.put("json_response", locationResponse);
                //longAdder.add(1);
                longAdder.getAndIncrement();
                long count = longAdder.longValue();
                if (count % 10000 == 0) {
                    log.info(String.format("已经解析%s条数据的位置信息", count));
                }
            }
        }
    }


    public String callForLocation(String url, String locationSourceInfo, int dealyTime) {
//        try {
//            Thread.sleep(50);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }
//        return "123";

        CloseableHttpClient client  = HttpClients.createDefault();
        HttpPost post = new HttpPost(url);

        RequestConfig config = RequestConfig.custom().setConnectTimeout(delay)
                .setConnectionRequestTimeout(delay * 3).setSocketTimeout(delay)
                .build();
        post.setConfig(config);
        post.setEntity(new StringEntity(locationSourceInfo, "UTF-8"));



        try (CloseableHttpResponse response = client.execute(post);){
            return EntityUtils.toString(response.getEntity(), "UTF-8");
        } catch (SocketTimeoutException te) {
            return "http request time out: " + delay + " ms";
        } catch (IOException e) {
            e.printStackTrace();
            return "error information: " + e.toString();
        }
    }
}
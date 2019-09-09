package com.meizu.bigdata.cetus.anyloader.java;

import com.google.common.collect.Lists;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.SocketTimeoutException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Logger;


public class ParseLocation {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(ParseLocation.class);
    int parrel;
    int delay;
    BlockingQueue<Map<String, String>[]>[] msgQueues;
    BlockingQueue<Map<String, String>[]>[] writeMsgQueues;
    //LongAdder longAdder = new LongAdder();
    AtomicLong longAdder = new AtomicLong();
    ExecutorService executor = null;
    CountDownLatch latch = null;

    public ParseLocation(int parrel, int delay, BlockingQueue<Map<String, String>[]>[] msgQueues, BlockingQueue<Map<String,String>[]>[] writeMsgQueues) {
        this.parrel = parrel;
        this.delay = delay;
        this.msgQueues = msgQueues;
        this.writeMsgQueues = writeMsgQueues;
    }



    public void parseLocation() {
        executor = Executors.newFixedThreadPool(parrel, new ParseThreadFacotry("parse-url"));
        latch = new CountDownLatch(parrel);
//        IntStream.range(0, parrel).forEach(
//          index -> {
//              BlockingQueue<Map<String, String>[]> msgQueue = msgQueues[index];
//              executor.submit(new ParseLocationRunnable(msgQueue, delay, longAdder, latch, writeMsgQueues, index));
//          }
//        );
        for (int index = 0; index < parrel; index++) {
            BlockingQueue<Map<String, String>[]> msgQueue = msgQueues[index];
            executor.submit(new ParseLocationRunnable(msgQueue, delay, longAdder, latch, writeMsgQueues, index));
        }

    }

    public void shutdown() {
        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        executor.shutdown();

        while (true) {
            try {
                if (executor.awaitTermination(1, TimeUnit.SECONDS)) {
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                break;
            }

        }
        log.info("call web url 数据处理结束");
    }



    public static void main(String[] args) {
        List<Map<String, String>> ls1 = new ArrayList<>();
        List<Map<String, String>> ls2 = new ArrayList<>();
        List<Map<String, String>> ls3 = new ArrayList<>();
        List<List<Map<String, String>>> fs = new ArrayList<>();
        fs.add(ls1);
        fs.add(ls2);
        fs.add(ls3);

        Map<String, String> e1 = new HashMap();
        e1.put("haiyang1", "1");

        Map<String, String> e2 = new HashMap<>();
        e2.put("haiyang2", "2");

        Map<String, String> e3 = new HashMap<>();
        e3.put("haiyang3", "3");

        Map<String, String> e4 = new HashMap<>();
        e4.put("haiyang4", "4");


        ls1.add(e1);
        ls1.add(e2);
        ls1.add(e3);
        ls1.add(e4);


        Map<String, String> f1 = new HashMap();
        f1.put("haiyang1", "1");

        Map<String, String> f2 = new HashMap<>();
        f2.put("haiyang2", "2");

        Map<String, String> f3 = new HashMap<>();
        f3.put("haiyang3", "3");

        Map<String, String> f4 = new HashMap<>();
        f4.put("haiyang4", "4");

        ls2.add(f1);
        ls2.add(f2);
        ls2.add(f3);
        ls2.add(f4);

        //List<Map<String, String>> l = fs.stream().flatMap(x -> x.stream()).collect(Collectors.toList());
        //System.out.println(l);
        log.info("end");

        log.error("1234");
        System.out.println(String.format("hello,%s, %s", "1 ", "2 "));
        System.out.println(MessageFormat.format("123{0}qwer", " good "));
    }


}


class ParseThreadFacotry implements ThreadFactory {

    private String prefix;

    AtomicInteger index = new AtomicInteger(0);

    public ParseThreadFacotry(String prefix) {
        this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
        String name = prefix + "-thread" + index.getAndIncrement();
        return new Thread(r, name);
    }
}
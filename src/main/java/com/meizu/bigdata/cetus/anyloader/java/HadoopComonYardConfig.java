package com.meizu.bigdata.cetus.anyloader.java;

import com.meizu.bigdata.hadoop.config.Location;

public class HadoopComonYardConfig extends YardConfig {
    public HadoopComonYardConfig(String url) {
        super(url);
    }
    private static final Object lock = new Object();
    static HadoopComonYardConfig config;
    private static final String url = Location.HADOOP_COMMON_V20;


    public static HadoopComonYardConfig getInstance() {
        if (config == null) {
            synchronized (lock) {
                config = new HadoopComonYardConfig(url);
                config.loadConfig();
            }
        }
        return config;
    }

    public static void main(String[] args) {
        LibraYardConfig config = LibraYardConfig.getInstance();
        System.out.println(config.get("cetus.libra.security.key"));
    }

}

package com.meizu.bigdata.cetus.anyloader.java;

class LibraYardConfig extends YardConfig {
    public LibraYardConfig(String url) {
        super(url);
    }
    private static final Object lock = new Object();
    static LibraYardConfig config;
    private static final String url = "http://yard.meizu.com/conf/bigdata-cetus/cetus-libra/V1.0";


    static LibraYardConfig getInstance() {
        if (config == null) {
            synchronized (lock) {
                config = new LibraYardConfig(url);
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

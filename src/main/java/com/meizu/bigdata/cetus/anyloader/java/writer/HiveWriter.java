package com.meizu.bigdata.cetus.anyloader.java.writer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

public interface HiveWriter  {
    public static final ReentrantLock lock = new ReentrantLock();

    public void initConf();

    public void initWriter();

    public void write();

    public void destroyWrite();

}

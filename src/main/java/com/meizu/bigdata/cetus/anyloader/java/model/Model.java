package com.meizu.bigdata.cetus.anyloader.java.model;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class Model {
    @Value("${hadoop.kerberos.switch}")
    private String hadoopSwitch;

    public String getHadoopSwitch() {
        return hadoopSwitch;
    }

    public String getWarehouse() {
        return warehouse;
    }

    @Value("${hive.warehouse}")
    private String warehouse;
}

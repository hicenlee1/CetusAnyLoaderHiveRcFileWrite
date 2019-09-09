package com.meizu.bigdata.cetus.anyloader.java;

import com.meizu.bigdata.hadoop.config.Key;
import com.meizu.bigdata.hadoop.config.Location;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.Properties;
import java.util.logging.Logger;

public class YardConfig {
    private static final org.slf4j.Logger log = LoggerFactory.getLogger(YardConfig.class);
    private String url;
    static YardConfig config;
    private static Properties prop;

    protected YardConfig(String url) {
        this.url = url;
    }


    public void loadConfig() {
        String response = getYardConfig();
        if (response != null) {
            prop = new Properties();
            try (Reader reader = new StringReader(response);){
                prop.load(new StringReader(response));
            } catch (IOException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        } else {
            log.error("获取YARD配置失败,url=" + url);
        }

    }
    private String getYardConfig() {
        CloseableHttpClient client = HttpClients.createDefault();
        HttpGet get = new HttpGet(url);

        try (CloseableHttpResponse response = client.execute(get);){
            return EntityUtils.toString(response.getEntity(), "UTF-8");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String get(String key) {
        return  (String) prop.get(key);
    }

}

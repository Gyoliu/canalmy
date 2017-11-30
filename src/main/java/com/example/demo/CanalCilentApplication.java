package com.example.demo;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.common.utils.AddressUtils;
import com.example.demo.client.AbstractCanalClient;

import java.net.InetSocketAddress;

/**
 * Created by liuxing on 2017/11/28.
 */
public class CanalCilentApplication extends AbstractCanalClient {

    public CanalCilentApplication(String destination){
        super(destination);
    }

    public static void main(String[] args) {
        // 根据ip，直接创建链接，无HA的功能
        String destination = "example";
        String ip = AddressUtils.getHostIp();
        CanalConnector connector = CanalConnectors.newSingleConnector(new InetSocketAddress(ip, 11111),destination,"","");

        final CanalCilentApplication clientTest = new CanalCilentApplication(destination);
        clientTest.setConnector(connector);
        clientTest.start();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                try {
                    logger.info("## stop the canal client");
                    clientTest.stop();
                } catch (Throwable e) {
                    logger.warn("##something goes wrong when stopping canal:", e);
                } finally {
                    logger.info("## canal client is down.");
                }
            }

        });
    }
}

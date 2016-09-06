package com.alibaba.otter.canal.plugins.impl;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import org.springframework.util.StringUtils;

import java.net.InetSocketAddress;

/**
 * Created by Adam.Wu on 2016/9/6.
 */
public class StandAloneConsumerDispatcher extends AbstractConsumerDispatcher {

    /**
     * canal主机
     */
    private String host;
    /**
     * 端口
     */
    private int port;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    @Override
    protected CanalConnector createConnector(String destination) {
        if (!StringUtils.hasText(host)) {
            host = "127.0.0.1";
        }
        return CanalConnectors.newSingleConnector(new InetSocketAddress(host, port), destination, "", "");
    }

}

package com.alibaba.otter.canal.plugins.impl;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;

/**
 * Created by Adam.Wu on 2016/9/6.
 */
public class ClusterConsumerDispatcher extends AbstractConsumerDispatcher {

    private String zkAddress;

    public String getZkAddress() {
        return zkAddress;
    }

    public void setZkAddress(String zkAddress) {
        this.zkAddress = zkAddress;
    }

    @Override
    protected CanalConnector createConnector(String destination) {
        return CanalConnectors.newClusterConnector(zkAddress, destination, "", "");
    }
}

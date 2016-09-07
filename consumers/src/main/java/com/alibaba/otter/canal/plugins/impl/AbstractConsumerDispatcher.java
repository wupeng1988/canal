package com.alibaba.otter.canal.plugins.impl;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.common.AbstractCanalLifeCycle;
import com.alibaba.otter.canal.plugins.Consumer;
import com.alibaba.otter.canal.plugins.ConsumerDispatcher;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.StringUtils;

import java.util.*;

/**
 * Created by Adam.Wu on 2016/9/6.
 */
public abstract class AbstractConsumerDispatcher extends AbstractCanalLifeCycle implements ConsumerDispatcher, InitializingBean {

    protected Map<String, List<Consumer>> consumerMap;

    /**
     * 消费者
     */
    protected List<Consumer> consumers;
    /**
     * 批量大小
     */
    protected int batchSize = 1000;

    /**
     * 获取消息线程
     */
    protected List<Thread> fetchers = new ArrayList<>();
    protected Map<String, CanalConnector> connectorMap;

    /**
     * start threads
     */
    @Override
    public void start() {
        super.start();
        connectorMap.forEach((destination, connector) -> {
            MessageFetcher fetcher = new MessageFetcher(connector, this.consumerMap.get(destination), batchSize);
            fetchers.add(new Thread(fetcher));
        });

        fetchers.forEach(Thread::start);
    }

    /**
     * stop threads
     */
    @Override
    public void stop() {
        super.stop();
        fetchers.forEach(Thread::interrupt);
        connectorMap.forEach((destination, connector) -> connector.disconnect());
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        /**
         * classify consumers by destination
         */
        if (consumers != null && consumers.size() > 0) {
            Map<String, List<Consumer>> map = new HashMap<>();
            consumers.forEach(consumer -> {
                List<Consumer> consumerList = map.get(consumer.destination());
                if (consumerList == null) {
                    consumerList = new ArrayList<>();
                    map.put(consumer.destination(), consumerList);
                }
                consumerList.add(consumer);
            });
            this.consumerMap = Collections.unmodifiableMap(map);

            /**
             * create canal connector for each destination
             */
            Map<String, CanalConnector> map2 = new HashMap<>();
            this.consumerMap.keySet().forEach(destination -> {
                if (map2.get(destination) == null) {
                    CanalConnector connector = createConnector(destination);
                    map2.put(destination, connector);
                }
            });
            this.connectorMap = Collections.unmodifiableMap(map2);
        }

        String enable = System.getProperty("canal.consumer.enable");
        if (!StringUtils.hasText(enable)) {
            enable = System.getenv("canal.consumer.enable");
        }
        if ("enable".equals(enable)) {
            start();
            Runtime.getRuntime().addShutdownHook(new Thread(this::stop));
        }
    }

    protected abstract CanalConnector createConnector(String destination);

    public List<Consumer> getConsumers() {
        return consumers;
    }

    public void setConsumers(List<Consumer> consumers) {
        this.consumers = consumers;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }
}

package com.alibaba.otter.canal.plugins.impl;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.plugins.Consumer;
import com.alibaba.otter.canal.protocol.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * thread for fetch messages from queue
 *
 * Created by Adam.Wu on 2016/9/6.
 */
public class MessageFetcher implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(MessageFetcher.class);

    private CanalConnector canalConnector;
    private List<Consumer> consumers;
    private int batchSize;

    public MessageFetcher(CanalConnector canalConnector, List<Consumer> consumers, int batchSize) {
        this.canalConnector = canalConnector;
        this.consumers = consumers;
        this.batchSize = batchSize;
    }

    public boolean isRunning() {
        return !Thread.currentThread().isInterrupted();
    }

    @Override
    public void run() {
        canalConnector.connect();
        canalConnector.subscribe();

        while (isRunning()) {
            try {
                Message message = canalConnector.getWithoutAck(batchSize);
                long batchId = message.getId();
                int size = message.getEntries().size();
                if (logger.isDebugEnabled())
                    logger.debug("read batchId {} , size {}", batchId, size);

                boolean ack = true;
                for (Consumer consumer : consumers) {
                    if (logger.isDebugEnabled())
                        logger.debug("executing consumer {}" , consumer.getClass());
                    Consumer.FeedBack feedBack = consumer.onMessage(message);
                    if (feedBack == Consumer.FeedBack.ROLLBACK) {
                        ack = false;
                    }
                    if (logger.isDebugEnabled())
                        logger.debug("executed consumer {} return signal {}",consumer.getClass() , feedBack);
                }

                if (ack) {
                    logger.debug("send ack signal ! batchId : {}", batchId);
                    canalConnector.ack(batchId);
                } else {
                    logger.debug("send rollbacl signal ! batchId: {}", batchId);
                    canalConnector.rollback(batchId);
                }
            } catch (Exception e) {
                logger.error("error fetching message !");
                logger.error(e.getMessage(), e);
            }
        }

        logger.info("message fetcher thread has bean exited !");
    }
}

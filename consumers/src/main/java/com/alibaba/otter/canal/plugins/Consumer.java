package com.alibaba.otter.canal.plugins;

import com.alibaba.otter.canal.protocol.Message;

/**
 * 消费者接口
 *
 * Created by Adam.Wu on 2016/9/6.
 */
public interface Consumer {

    enum FeedBack {
        ACK,
        ROLLBACK;
    }

    String destination();

    FeedBack onMessage(Message message);
}

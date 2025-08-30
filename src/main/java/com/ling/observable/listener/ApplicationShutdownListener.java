package com.ling.observable.listener;

import com.ling.observable.observable.service.SubscriptionConsumerService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextClosedEvent;
import org.springframework.stereotype.Component;

/**
 * 应用关闭监听器
 * 在Spring应用上下文关闭时执行，用于清理资源
 */
@Component
public class ApplicationShutdownListener implements ApplicationListener<ContextClosedEvent> {

    private static final Logger logger = LogManager.getLogger(ApplicationShutdownListener.class);

    @Autowired
    private SubscriptionConsumerService subscriptionConsumerService;

    /**
     * 当应用上下文关闭时调用
     *
     * @param event 上下文关闭事件
     */
    @Override
    public void onApplicationEvent(ContextClosedEvent event) {
        logger.info("应用正在关闭，开始清理订阅消费者服务资源...");
        subscriptionConsumerService.shutdown();
        logger.info("订阅消费者服务资源清理完成");
    }
}
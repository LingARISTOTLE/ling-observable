package com.ling.observable.listener;

import com.ling.observable.observable.service.ObservableService;
import com.ling.observable.observable.service.SubscriptionConsumerService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Component;

/**
 * 应用启动监听器
 * 在Spring应用上下文刷新完成后执行，用于初始化订阅消费者服务
 */
@Component
public class ApplicationStartupListener implements ApplicationListener<ContextRefreshedEvent> {
    
    private static final Logger logger = LogManager.getLogger(ApplicationStartupListener.class);
    
    @Autowired
    private SubscriptionConsumerService subscriptionConsumerService;
    
    @Autowired
    private ObservableService observableService;
    
    /**
     * 当应用上下文刷新完成时调用
     * 
     * @param event 上下文刷新事件
     */
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        // 确保只在根应用上下文中执行一次
        if (event.getApplicationContext().getParent() == null) {
            logger.info("应用启动完成，开始初始化订阅消费者服务...");
            
            // 示例：创建一个测试用的Observable和订阅
            try {
                // 创建Observable
                Long observableId = observableService.createObservable();
                logger.info("创建测试Observable，ID: {}", observableId);
                
                // 添加一些测试数据
                observableService.addData(observableId, "测试数据1");
                observableService.addData(observableId, "测试数据2");
                
                // 创建订阅
                Long subscriptionId = observableService.subscribe(observableId);
                logger.info("创建测试订阅，ID: {}", subscriptionId);
                
                // 添加消费者任务
                subscriptionConsumerService.addConsumerTask(observableId, subscriptionId);
                
                // 添加更多测试数据
                new Thread(() -> {
                    try {
                        Thread.sleep(2000);
                        observableService.addData(observableId, "延迟测试数据3");
                        
                        Thread.sleep(10000);
                        observableService.addData(observableId, "延迟测试数据4");
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }).start();
                
            } catch (Exception e) {
                logger.error("初始化测试订阅时出错: {}", e.getMessage(), e);
            }
        }
    }
}
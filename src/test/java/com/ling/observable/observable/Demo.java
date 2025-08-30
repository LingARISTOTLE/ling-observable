package com.ling.observable.observable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Arrays;
import java.util.List;

/**
 * 观察者模式演示程序
 * 展示如何使用Observable、Subscriber和Subscription实现观察者模式
 */
public class Demo {
    
    private static final Logger logger = LogManager.getLogger(Demo.class);
    
    public static void main(String[] args) {
        logger.info("=== 观察者模式演示 ===");
        
        // 创建一个可观察的数据源 - 一组整数
        List<Integer> numbers = Arrays.asList(1, 2, 3, 4, 5);
        
        // 创建Observable对象
        logger.info("创建Observable对象...");
        Observable<Integer> observable = new Observable<>(numbers);
        
        try {
            // 等待一小段时间，让Observable初始化
            Thread.sleep(100);
            
            // 订阅者1订阅
            logger.info("订阅者1订阅...");
            Subscription<Integer> subscription1 = observable.subscribe();
            
            // 订阅者2订阅
            logger.info("订阅者2订阅...");
            Subscription<Integer> subscription2 = observable.subscribe();
            
            // 启动线程处理订阅者1的消息
            Thread subscriber1Thread = new Thread(() -> {
                try {
                    logger.info("订阅者1开始接收数据...");
                    while (!subscription1.isEmpty() || !Thread.currentThread().isInterrupted()) {
                        try {
                            Integer item = subscription1.take();
                            logger.info("订阅者1接收到: {}", item);
                            Thread.sleep(500); // 模拟处理时间
                        } catch (InterruptedException e) {
                            // 线程被中断时退出循环
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                    logger.info("订阅者1处理完成");
                } catch (Exception e) {
                    logger.error("订阅者1处理数据时出错", e);
                }
            });
            
            // 启动线程处理订阅者2的消息
            Thread subscriber2Thread = new Thread(() -> {
                try {
                    logger.info("订阅者2开始接收数据...");
                    while (!subscription2.isEmpty() || !Thread.currentThread().isInterrupted()) {
                        try {
                            Integer item = subscription2.take();
                            logger.info("订阅者2接收到: {}", item);
                            Thread.sleep(300); // 模拟处理时间
                        } catch (InterruptedException e) {
                            // 线程被中断时退出循环
                            Thread.currentThread().interrupt();
                            break;
                        }
                    }
                    logger.info("订阅者2处理完成");
                } catch (Exception e) {
                    logger.error("订阅者2处理数据时出错", e);
                }
            });
            
            // 启动处理线程
            subscriber1Thread.start();
            subscriber2Thread.start();
            
            // 等待所有处理完成
            subscriber1Thread.join();
            subscriber2Thread.join();
            
            logger.info("所有处理完成");
            
        } catch (Exception e) {
            logger.error("演示过程中发生错误", e);
        }
    }
}
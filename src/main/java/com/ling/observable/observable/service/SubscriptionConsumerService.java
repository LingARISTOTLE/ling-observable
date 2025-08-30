package com.ling.observable.observable.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import jakarta.annotation.PostConstruct;

import java.util.concurrent.*;

/**
 * 订阅消费者服务类
 * 该服务在应用启动后自动启动，专门用于消费订阅者队列中的数据
 * 使用线程池处理多个订阅者的数据消费
 */
@Service
public class SubscriptionConsumerService {

    private static final Logger logger = LogManager.getLogger(SubscriptionConsumerService.class);

    @Autowired
    private ObservableService observableService;

    /**
     * 线程池，用于处理多个订阅者的数据消费
     */
    private ExecutorService executorService;

    /**
     * 用于存储正在处理的订阅任务
     */
    private final ConcurrentHashMap<String, Future<?>> consumerTasks = new ConcurrentHashMap<>();

    /**
     * 应用启动后自动执行的初始化方法
     * 初始化线程池并启动消费者任务
     */
    @PostConstruct
    public void init() {
        // 创建固定大小的线程池
        executorService = Executors.newFixedThreadPool(10);
        logger.info("SubscriptionConsumerService 已启动，线程池已初始化");
    }

    /**
     * 添加订阅消费者任务
     *
     * @param observableId   Observable ID
     * @param subscriptionId Subscription ID
     */
    public void addConsumerTask(Long observableId, Long subscriptionId) {
        String taskKey = observableId + "_" + subscriptionId;

        // 检查任务是否已经存在
        if (consumerTasks.containsKey(taskKey)) {
            logger.warn("任务已存在: {}", taskKey);
            return;
        }

        // 提交新的消费者任务到线程池
        Future<?> future = executorService.submit(() -> {
            logger.info("开始消费订阅数据 - Observable: {}, Subscription: {}", observableId, subscriptionId);
            consumeData(observableId, subscriptionId);
        });

        // 存储任务引用
        consumerTasks.put(taskKey, future);
        logger.info("已添加消费者任务: {}", taskKey);
    }

    /**
     * 消费订阅数据的核心方法
     *
     * @param observableId   Observable ID
     * @param subscriptionId Subscription ID
     */
    private void consumeData(Long observableId, Long subscriptionId) {
        String taskKey = observableId + "_" + subscriptionId;

        try {
            while (!executorService.isShutdown()) {
                try {
                    // 从订阅中获取数据（阻塞操作）
                    Object data = observableService.takeData(observableId, subscriptionId);

                    // 打印订阅的内容
                    logger.info("订阅消费者 [{}] 接收到数据: {}", taskKey, data);

                    // 线程停顿4秒，模拟消费者处理数据
                    Thread.sleep(4000);
                } catch (InterruptedException e) {
                    logger.info("消费者任务被中断: {}", taskKey);
                    Thread.currentThread().interrupt();
                    break;
                } catch (Exception e) {
                    logger.error("消费者任务出现异常 [{}]: {}", taskKey, e.getMessage(), e);
                    // 继续尝试消费数据
                }
            }
        } finally {
            // 从任务列表中移除已完成的任务
            consumerTasks.remove(taskKey);
            logger.info("消费者任务已完成并移除: {}", taskKey);
        }
    }

    /**
     * 移除并停止指定的消费者任务
     *
     * @param observableId   Observable ID
     * @param subscriptionId Subscription ID
     */
    public void removeConsumerTask(Long observableId, Long subscriptionId) {
        String taskKey = observableId + "_" + subscriptionId;
        Future<?> future = consumerTasks.get(taskKey);

        if (future != null) {
            // 尝试取消任务
            future.cancel(true);
            consumerTasks.remove(taskKey);
            logger.info("已移除消费者任务: {}", taskKey);
        } else {
            logger.warn("未找到消费者任务: {}", taskKey);
        }
    }

    /**
     * 关闭服务并清理资源
     */
    public void shutdown() {
        if (executorService != null && !executorService.isShutdown()) {
            logger.info("正在关闭订阅消费者服务...");
            executorService.shutdown();
            try {
                // 等待现有任务完成
                if (!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
            logger.info("订阅消费者服务已关闭");
        }
    }

    /**
     * 在对象销毁前执行清理工作
     */
    public void destroy() {
        shutdown();
    }
}
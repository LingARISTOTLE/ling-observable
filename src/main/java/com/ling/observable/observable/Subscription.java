package com.ling.observable.observable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingQueue;

/**
 * 订阅对象类
 * 提供订阅者接收数据的接口
 *
 * @param <T> 数据类型
 */
public class Subscription<T> {
    
    private static final Logger logger = LogManager.getLogger(Subscription.class);
    
    /**
     * 关联的缓冲队列
     * 用于存储和传递数据项
     */
    private final BlockingQueue<T> queue;

    /**
     * 构造函数
     *
     * @param queue 缓冲队列
     */
    public Subscription(BlockingQueue<T> queue) {
        this.queue = queue;
        logger.debug("创建新的Subscription实例");
    }

    /**
     * 获取数据项（阻塞方法）
     * 如果队列为空，该方法会阻塞直到有数据可用
     *
     * @return 取出的数据项
     * @throws InterruptedException 如果线程被中断
     */
    public T take() throws InterruptedException {
        T item = queue.take();
        logger.debug("从队列中取出数据项: {}", item);
        return item;
    }

    /**
     * 检查队列是否为空
     *
     * @return 如果队列为空返回true，否则返回false
     */
    public boolean isEmpty() {
        boolean empty = queue.isEmpty();
        logger.debug("检查队列是否为空: {}", empty);
        return empty;
    }
}
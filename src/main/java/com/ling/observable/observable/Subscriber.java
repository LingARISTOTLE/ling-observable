package com.ling.observable.observable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 订阅者类
 * 负责接收和缓存来自Observable的数据
 * 
 * @param <T> 数据类型
 */
public class Subscriber<T> {
    
    private static final Logger logger = LogManager.getLogger(Subscriber.class);
    
    /**
     * 使用阻塞队列作为数据缓冲区，线程安全
     * 当队列为空时，take()方法会阻塞直到有数据可用
     */
    private final BlockingQueue<T> buffer = new LinkedBlockingQueue<>();
    
    /**
     * 标记订阅者是否已关闭
     * 使用AtomicBoolean确保原子操作
     */
    private final AtomicBoolean closed = new AtomicBoolean(false);
    
    /**
     * 订阅对象，用于与Observable交互
     */
    private final Subscription<T> subscription = new Subscription<>(buffer);

    /**
     * 接收数据项
     * 
     * @param item 数据项
     */
    public void emit(T item) {
        // 只有在未关闭状态下才接收数据
        if (!closed.get()) {
            // 将数据放入缓冲区
            buffer.offer(item);
            logger.debug("接收到数据项: {}", item);
        } else {
            logger.warn("尝试向已关闭的订阅者发送数据项: {}", item);
        }
    }

    /**
     * 关闭订阅者
     * 清理资源并标记为已关闭
     */
    public void close() {
        // 使用CAS操作确保只执行一次关闭操作
        if (closed.compareAndSet(false, true)) {
            // 清空缓冲区
            buffer.clear();
            logger.debug("订阅者已关闭，缓冲区已清空");
        }
    }

    /**
     * 获取订阅对象
     * 
     * @return Subscription对象
     */
    public Subscription<T> getSubscription() {
        return subscription;
    }
}
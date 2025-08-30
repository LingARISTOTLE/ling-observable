package com.ling.observable.observable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 可观察对象类
 * 负责管理订阅者并推送数据
 *
 * @param <T> 数据类型
 * @author Ling
 */
public class Observable<T> {
    
    private static final Logger logger = LogManager.getLogger(Observable.class);
    
    /**
     * 数据源，用于提供需要推送的数据
     * 修改为支持动态添加数据的列表
     */
    private final List<T> dataList;

    /**
     * 存储订阅者映射关系 ConcurrentHashMap保证线程安全
     * Key: Subscription对象，Value: Subscriber对象
     */
    private final ConcurrentHashMap<Subscription<T>, Subscriber<T>> listeners;

    /**
     * 标记Observable是否已完成所有数据推送
     * 使用AtomicBoolean确保原子操作
     */
    private final AtomicBoolean done = new AtomicBoolean(false);

    /**
     * 读写锁，用于保护done状态的访问
     * 读锁：允许多个线程同时读取状态
     * 写锁：确保只有一个线程可以修改状态
     */
    private final ReadWriteLock doneLock = new ReentrantReadWriteLock();

    /**
     * 构造函数
     *
     * @param dataList 数据列表
     */
    public Observable(List<T> dataList) {
        this.dataList = dataList;
        this.listeners = new ConcurrentHashMap<>();
        // 启动处理线程，开始推送数据
        new Thread(this::process).start();
        logger.debug("创建新的Observable实例");
    }

    /**
     * 处理数据并推送给所有订阅者
     * 这是Observable的核心处理逻辑
     * 修改为：当没有订阅者时阻塞，有订阅者时开始处理数据
     */
    private void process() {
        try {
            int currentIndex = 0;
            while (!done.get()) {
                // 等待至少有一个订阅者
                while (listeners.isEmpty() && !done.get()) {
                    try {
                        // 短暂休眠，避免忙等待
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }

                // 当有订阅者且未完成时，推送新数据
                if (!done.get() && currentIndex < dataList.size()) {
                    T item = dataList.get(currentIndex);
                    currentIndex++;
                    
                    // 推送数据给所有订阅者
                    listeners.values().forEach(subscriber -> subscriber.emit(item));
                    logger.debug("推送数据项: {}", item);
                    
                    // 短暂休眠以控制推送速度
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                } else if (currentIndex >= dataList.size()) {
                    // 没有更多数据，短暂休眠等待新数据
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        } finally {
            // 处理完成后关闭所有订阅者
            close();
        }
    }

    /**
     * 动态添加数据到Observable
     * 
     * @param item 要添加的数据项
     */
    public void addData(T item) {
        dataList.add(item);
        logger.debug("添加新数据项: {}", item);
    }

    /**
     * 关闭Observable，通知所有订阅者数据推送已完成
     */
    private void close() {
        // 获取写锁，确保状态修改的独占性
        doneLock.writeLock().lock();
        try {
            // 使用CAS操作确保只执行一次关闭操作
            if (done.compareAndSet(false, true)) {
                // 关闭所有订阅者
                listeners.values().forEach(Subscriber::close);
                logger.info("Observable已关闭，通知所有订阅者");
            }
        } finally {
            // 释放写锁
            doneLock.writeLock().unlock();
        }
    }

    /**
     * 订阅方法
     *
     * @return Subscription对象，用于接收数据
     * @throws Exception 如果Observable已关闭则抛出异常
     */
    public Subscription<T> subscribe() throws Exception {
        // 获取读锁，允许多个订阅者同时订阅
        doneLock.readLock().lock();
        try {
            // 检查Observable是否已经关闭
            if (done.get()) {
                logger.warn("尝试订阅已关闭的Observable");
                throw new Exception("Observable is closed");
            }

            // 创建新的订阅者
            Subscriber<T> subscriber = new Subscriber<>();
            // 将订阅者添加到监听列表中
            listeners.put(subscriber.getSubscription(), subscriber);
            logger.debug("新增订阅者，当前订阅者数量: {}", listeners.size());
            // 返回订阅对象
            return subscriber.getSubscription();
        } finally {
            // 释放读锁
            doneLock.readLock().unlock();
        }
    }

    /**
     * 取消订阅
     *
     * @param subscription 要取消的订阅对象
     */
    public void unsubscribe(Subscription<T> subscription) {
        // 从监听列表中移除订阅者
        Subscriber<T> subscriber = listeners.remove(subscription);
        if (subscriber != null) {
            // 关闭订阅者
            subscriber.close();
            logger.debug("取消订阅，当前订阅者数量: {}", listeners.size());
        } else {
            logger.warn("尝试取消不存在的订阅");
        }
    }
    
    /**
     * 获取当前订阅者数量
     * 
     * @return 订阅者数量
     */
    public int getSubscriberCount() {
        return listeners.size();
    }
}
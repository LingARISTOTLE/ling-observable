package com.ling.observable.observable.service;

import com.ling.observable.observable.Observable;
import com.ling.observable.observable.Subscription;
import org.springframework.stereotype.Service;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Observable服务类
 * 管理多个Observable实例，支持动态添加数据和订阅者
 */
@Service
public class ObservableService {

    private static final Logger logger = LogManager.getLogger(ObservableService.class);

    /**
     * 存储所有的Observable实例
     * Key: Observable ID
     * Value: Observable实例
     */
    private final ConcurrentHashMap<Long, Observable<Object>> observables = new ConcurrentHashMap<>();

    /**
     * 存储所有活动的订阅
     * Key: Observable ID
     * Value: 订阅列表
     */
    private final ConcurrentHashMap<Long, List<Subscription<Object>>> subscriptions = new ConcurrentHashMap<>();

    /**
     * Observable ID生成器
     */
    private final AtomicLong observableIdGenerator = new AtomicLong(0);

    /**
     * Subscription ID生成器
     */
    private final AtomicLong subscriptionIdGenerator = new AtomicLong(0);

    /**
     * 创建一个新的Observable实例
     *
     * @return 新创建的Observable ID
     */
    public Long createObservable() {
        Long id = observableIdGenerator.incrementAndGet();
        // 使用CopyOnWriteArrayList以支持线程安全的动态数据添加
        Observable<Object> observable = new Observable<>(new CopyOnWriteArrayList<>());
        observables.put(id, observable);
        subscriptions.put(id, new CopyOnWriteArrayList<>());
        logger.debug("创建新的Observable实例，ID: {}", id);
        return id;
    }

    /**
     * 向指定的Observable添加数据
     *
     * @param observableId Observable ID
     * @param data         要添加的数据
     * @return 是否添加成功
     */
    public boolean addData(Long observableId, Object data) {
        Observable<Object> observable = observables.get(observableId);
        if (observable == null) {
            logger.warn("尝试向不存在的Observable添加数据，ID: {}", observableId);
            return false;
        }

        // 添加数据到Observable
        observable.addData(data);
        logger.debug("向Observable添加数据，ID: {}，数据: {}", observableId, data);
        return true;
    }

    /**
     * 为指定的Observable创建订阅
     *
     * @param observableId Observable ID
     * @return Subscription ID，如果Observable不存在则返回null
     * @throws Exception 如果订阅失败
     */
    public Long subscribe(Long observableId) throws Exception {
        Observable<Object> observable = observables.get(observableId);
        if (observable == null) {
            logger.warn("尝试为不存在的Observable创建订阅，ID: {}", observableId);
            return null;
        }

        Subscription<Object> subscription = observable.subscribe();
        Long subscriptionId = subscriptionIdGenerator.incrementAndGet();

        // 将Subscription存储在列表中
        subscriptions.get(observableId).add(subscription);
        logger.debug("为Observable创建新订阅，Observable ID: {}，Subscription ID: {}", observableId, subscriptionId);
        return subscriptionId;
    }

    /**
     * 获取指定订阅中的下一个数据项
     *
     * @param observableId   Observable ID
     * @param subscriptionId Subscription ID
     * @return 数据项
     * @throws InterruptedException 如果线程被中断
     */
    public Object takeData(Long observableId, Long subscriptionId) throws InterruptedException {
        List<Subscription<Object>> subs = subscriptions.get(observableId);
        if (subs == null || subscriptionId >= subs.size()) {
            logger.warn("尝试从不存在的订阅获取数据，Observable ID: {}，Subscription ID: {}", observableId, subscriptionId);
            return null;
        }

        Subscription<Object> subscription = subs.get(subscriptionId.intValue());
        if (subscription == null) {
            logger.warn("订阅对象为空，Observable ID: {}，Subscription ID: {}", observableId, subscriptionId);
            return null;
        }

        return subscription.take();
    }

    /**
     * 检查指定订阅的队列是否为空
     *
     * @param observableId   Observable ID
     * @param subscriptionId Subscription ID
     * @return 如果队列为空返回true，否则返回false
     */
    public boolean isSubscriptionEmpty(Long observableId, Long subscriptionId) {
        List<Subscription<Object>> subs = subscriptions.get(observableId);
        if (subs == null || subscriptionId >= subs.size()) {
            return true;
        }

        Subscription<Object> subscription = subs.get(subscriptionId.intValue());
        return subscription == null || subscription.isEmpty();
    }

    /**
     * 获取指定Observable的订阅者数量
     *
     * @param observableId Observable ID
     * @return 订阅者数量
     */
    public Integer getSubscriberCount(Long observableId) {
        Observable<Object> observable = observables.get(observableId);
        if (observable == null) {
            return null;
        }
        return observable.getSubscriberCount();
    }

    /**
     * 获取指定的Subscription对象
     *
     * @param observableId   Observable ID
     * @param subscriptionId Subscription ID
     * @return Subscription对象
     */
    public Subscription<Object> getSubscription(Long observableId, Long subscriptionId) {
        List<Subscription<Object>> subs = subscriptions.get(observableId);
        if (subs == null || subscriptionId >= subs.size()) {
            return null;
        }

        return subs.get(subscriptionId.intValue());
    }
}
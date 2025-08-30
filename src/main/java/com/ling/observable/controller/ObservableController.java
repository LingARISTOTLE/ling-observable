package com.ling.observable.controller;

import com.ling.observable.observable.service.ObservableService;
import com.ling.observable.observable.service.SubscriptionConsumerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Observable控制器
 * 提供REST API接口用于管理Observable实例和订阅
 */
@RestController
@RequestMapping("/api/observable")
public class ObservableController {
    
    private static final Logger logger = LogManager.getLogger(ObservableController.class);
    
    @Autowired
    private ObservableService observableService;
    
    @Autowired
    private SubscriptionConsumerService subscriptionConsumerService;
    
    /**
     * 存储Subscription ID到Observable ID的映射关系
     */
    private final Map<Long, Long> subscriptionToObservableMap = new ConcurrentHashMap<>();
    
    /**
     * 创建一个新的Observable实例
     * 
     * @return 包含新创建的Observable ID的响应
     */
    @PostMapping("/create")
    public ResponseEntity<Map<String, Object>> createObservable() {
        Long observableId = observableService.createObservable();
        logger.info("创建新的Observable实例，ID: {}", observableId);
        
        Map<String, Object> response = new HashMap<>();
        response.put("success", true);
        response.put("observableId", observableId);
        return ResponseEntity.ok(response);
    }
    
    /**
     * 向指定的Observable添加数据
     * 
     * @param observableId Observable ID
     * @param data 要添加的数据
     * @return 操作结果
     */
    @PostMapping("/{observableId}/data")
    public ResponseEntity<Map<String, Object>> addData(
            @PathVariable Long observableId,
            @RequestBody Map<String, Object> data) {
        
        try {
            Object item = data.get("data");
            boolean success = observableService.addData(observableId, item);
            logger.info("向Observable添加数据，ID: {}，数据: {}，结果: {}", observableId, item, success);
            
            if (success) {
                Map<String, Object> response = new HashMap<>();
                response.put("success", true);
                response.put("message", "Data added successfully");
                return ResponseEntity.ok(response);
            } else {
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "Observable not found");
                return ResponseEntity.badRequest().body(response);
            }
        } catch (Exception e) {
            logger.error("添加数据时发生异常，ID: {}，异常: {}", observableId, e.getMessage(), e);
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "Failed to add data: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }
    
    /**
     * 获取指定Observable的订阅者数量
     *
     * @param observableId Observable ID
     * @return 订阅者数量
     */
    @GetMapping("/{observableId}/subscribers")
    public ResponseEntity<Map<String, Object>> getSubscriberCount(@PathVariable Long observableId) {
        try {
            Integer count = observableService.getSubscriberCount(observableId);
            logger.debug("获取Observable的订阅者数量，ID: {}，数量: {}", observableId, count);
            
            if (count == null) {
                logger.warn("尝试获取不存在的Observable的订阅者数量，ID: {}", observableId);
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "Observable not found");
                return ResponseEntity.badRequest().body(response);
            }
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("count", count);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("获取订阅者数量时发生异常，ID: {}，异常: {}", observableId, e.getMessage(), e);
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "Failed to get subscriber count: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }
    
    /**
     * 为指定的Observable创建订阅
     * 
     * @param observableId Observable ID
     * @return 包含新创建的Subscription ID的响应
     */
    @PostMapping("/{observableId}/subscribe")
    public ResponseEntity<Map<String, Object>> subscribe(@PathVariable Long observableId) {
        try {
            Long subscriptionId = observableService.subscribe(observableId);
            logger.info("为Observable创建订阅，Observable ID: {}，Subscription ID: {}", observableId, subscriptionId);
            
            if (subscriptionId == null) {
                logger.warn("尝试为不存在的Observable创建订阅，ID: {}", observableId);
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "Observable not found");
                return ResponseEntity.badRequest().body(response);
            }
            
            // 建立Subscription ID到Observable ID的映射
            subscriptionToObservableMap.put(subscriptionId, observableId);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("subscriptionId", subscriptionId);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("创建订阅时发生异常，ID: {}，异常: {}", observableId, e.getMessage(), e);
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "Failed to subscribe: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }
    
    /**
     * 为指定的订阅启动消费者任务
     * 
     * @param observableId Observable ID
     * @param subscriptionId Subscription ID
     * @return 操作结果
     */
    @PostMapping("/{observableId}/subscription/{subscriptionId}/consume")
    public ResponseEntity<Map<String, Object>> startConsuming(
            @PathVariable Long observableId,
            @PathVariable Long subscriptionId) {
        
        try {
            // 检查订阅是否存在
            if (observableService.getSubscription(observableId, subscriptionId) == null) {
                logger.warn("尝试为不存在的订阅启动消费者任务，Observable ID: {}，Subscription ID: {}", observableId, subscriptionId);
                Map<String, Object> response = new HashMap<>();
                response.put("success", false);
                response.put("message", "Subscription not found");
                return ResponseEntity.badRequest().body(response);
            }
            
            // 启动消费者任务
            subscriptionConsumerService.addConsumerTask(observableId, subscriptionId);
            logger.info("为订阅启动消费者任务，Observable ID: {}，Subscription ID: {}", observableId, subscriptionId);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("message", "Consumer task started successfully");
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("启动消费者任务时发生异常，Observable ID: {}，Subscription ID: {}，异常: {}", 
                observableId, subscriptionId, e.getMessage(), e);
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "Failed to start consumer task: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }
    
    /**
     * 从指定的订阅中获取数据
     * 
     * @param observableId Observable ID
     * @param subscriptionId Subscription ID
     * @return 数据项
     */
    @GetMapping("/{observableId}/subscription/{subscriptionId}/data")
    public ResponseEntity<Map<String, Object>> getData(
            @PathVariable Long observableId,
            @PathVariable Long subscriptionId) {
        
        try {
            Object data = observableService.takeData(observableId, subscriptionId);
            logger.debug("从订阅获取数据，Observable ID: {}，Subscription ID: {}，数据: {}", observableId, subscriptionId, data);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("data", data);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("获取数据时发生异常，Observable ID: {}，Subscription ID: {}，异常: {}", 
                observableId, subscriptionId, e.getMessage(), e);
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "Failed to get data: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }
    
    /**
     * 检查指定订阅的队列是否为空
     * 
     * @param observableId Observable ID
     * @param subscriptionId Subscription ID
     * @return 检查结果
     */
    @GetMapping("/{observableId}/subscription/{subscriptionId}/empty")
    public ResponseEntity<Map<String, Object>> isSubscriptionEmpty(
            @PathVariable Long observableId,
            @PathVariable Long subscriptionId) {
        
        try {
            boolean isEmpty = observableService.isSubscriptionEmpty(observableId, subscriptionId);
            logger.debug("检查订阅队列是否为空，Observable ID: {}，Subscription ID: {}，结果: {}", observableId, subscriptionId, isEmpty);
            
            Map<String, Object> response = new HashMap<>();
            response.put("success", true);
            response.put("empty", isEmpty);
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            logger.error("检查订阅队列是否为空时发生异常，Observable ID: {}，Subscription ID: {}，异常: {}", 
                observableId, subscriptionId, e.getMessage(), e);
            Map<String, Object> response = new HashMap<>();
            response.put("success", false);
            response.put("message", "Failed to check subscription: " + e.getMessage());
            return ResponseEntity.badRequest().body(response);
        }
    }
}
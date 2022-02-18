package com.roncoo.eshop.cache.kafka;

import com.alibaba.fastjson.JSONObject;
import com.roncoo.eshop.cache.model.ProductInfo;
import com.roncoo.eshop.cache.model.ShopInfo;
import com.roncoo.eshop.cache.service.CacheService;
import com.roncoo.eshop.cache.spring.SpringContext;

import com.roncoo.eshop.cache.zk.ZooKeeperSession;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * kafka消息处理线程
 * @author Administrator
 *
 */
public class KafkaMessageProcessor implements Runnable{
    private KafkaStream kafkaStream;
    private CacheService cacheService;

    public KafkaMessageProcessor(KafkaStream kafkaStream) {
        this.kafkaStream = kafkaStream;
        this.cacheService = (CacheService) SpringContext.getApplicationContext()
                .getBean("cacheService");
    }

    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = kafkaStream.iterator();
        while (it.hasNext()) {
            String message = new String(it.next().message());

            // 首先将message转换成json对象
            JSONObject messageJSONObject = JSONObject.parseObject(message);

            // 从这里提取出消息对应的服务的标识
            String serviceId = messageJSONObject.getString("serviceId");

            // 如果是商品信息服务
            if("productInfoService".equals(serviceId)) {
                processProductInfoChangeMessage(messageJSONObject);
            } else if("shopInfoService".equals(serviceId)) {
                processShopInfoChangeMessage(messageJSONObject);
            }
        }
    }

    /**
     * 处理商品信息变更的消息
     * @param messageJSONObject 消息体
     */
    private void processProductInfoChangeMessage(JSONObject messageJSONObject) {
        // 提取出商品id
        Long productId = messageJSONObject.getLong("productId");
        // 模拟查询数据库的数据
        String productInfoJSON = "{\"id\": 1, \"name\": \"iphone7手机\", \"price\": 5599, \"pictureList\":\"a.jpg,b.jpg\", \"specification\": \"iphone7的规格\", \"service\": \"iphone7的售后服务\", \"color\": \"红色,白色,黑色\", \"size\": \"5.5\", \"shopId\": 1, \"modifiedTime\": \"2017-01-01 12:00:00\"}";
        ProductInfo productInfo = JSONObject.parseObject(productInfoJSON, ProductInfo.class);
        // 获取分布式锁
        ZooKeeperSession.getInstance().acquireDistributeLock(productId);
        final ProductInfo productInfoFromRedisCache = cacheService.getProductInfoFromRedisCache(productId);
        if (productInfoFromRedisCache != null) {
            // 比较redis中读取出来的modifyTime和kafka中数据里modifyTime
            final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");
            // 消息队列中productInfo的modifyTime早于redis中productInfo的modifyTime
            if (!LocalDateTime.parse(productInfo.getModifyTime(), dateTimeFormatter).isAfter(LocalDateTime.parse(productInfoFromRedisCache.getModifyTime(), dateTimeFormatter))) {
                System.out.println("消息队列中productInfo的modifyTime早于redis中productInfo的modifyTime");
                return;
            }
        } else {
            System.out.println("Redis中没有productId=" + productId + "的相关信息");
        }
        cacheService.saveProductInfo2LocalCache(productInfo);
        System.out.println("===================获取刚保存到本地缓存的商品信息：" + cacheService.getProductInfoFromLocalCache(productId));
        cacheService.saveProductInfo2ReidsCache(productInfo);
        // 处理完毕释放分布式锁
        ZooKeeperSession.getInstance().releaseDistributeLock(productId);
    }

    /**
     * 处理店铺信息变更的消息
     * @param messageJSONObject 消息体
     */
    private void processShopInfoChangeMessage(JSONObject messageJSONObject) {
        // 提取出商品id
        // Long productId = messageJSONObject.getLong("productId");
        Long shopId = messageJSONObject.getLong("shopId");
        // 模拟查询数据库的数据
        String shopInfoJSON = "{\"id\": 1, \"name\": \"小王的手机店\", \"level\": 5, \"goodCommentRate\":0.99}";
        ShopInfo shopInfo = JSONObject.parseObject(shopInfoJSON, ShopInfo.class);
        // 获取分布式锁
        ZooKeeperSession.getInstance().acquireDistributeLock(shopId);
        final ShopInfo shopInfoFromRedisCache = cacheService.getShopInfoFromReidsCache(shopId);
        if (shopInfoFromRedisCache != null) {
            // 比较redis中读取出来的modifyTime和kafka中数据里modifyTime
            final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");
            // 消息队列中productInfo的modifyTime早于redis中productInfo的modifyTime
            if (!LocalDateTime.parse(shopInfo.getModifyTime(), dateTimeFormatter).isAfter(LocalDateTime.parse(shopInfoFromRedisCache.getModifyTime(), dateTimeFormatter))) {
                System.out.println("消息队列中shopInfo的modifyTime早于redis中shopInfo的modifyTime");
                return;
            }
        } else {
            System.out.println("Redis中没有shopId=" + shopId + "的相关信息");
        }
        cacheService.saveShopInfo2LocalCache(shopInfo);
        System.out.println("===================获取刚保存到本地缓存的店铺信息：" + cacheService.getShopInfoFromLocalCache(shopId));
        cacheService.saveShopInfo2ReidsCache(shopInfo);
        // 处理完毕释放分布式锁
        ZooKeeperSession.getInstance().releaseDistributeLock(shopId);
    }
}

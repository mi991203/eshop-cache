package com.roncoo.eshop.cache.rebuild;

import com.roncoo.eshop.cache.model.ProductInfo;
import com.roncoo.eshop.cache.service.CacheService;
import com.roncoo.eshop.cache.spring.SpringContext;
import com.roncoo.eshop.cache.zk.ZooKeeperSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class RebuildCacheThread implements Runnable {

    @Override
    public void run() {
        final DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd hh:mm:ss");
        final RebuildCacheQueue rebuildCacheQueue = RebuildCacheQueue.getInstance();
        final ZooKeeperSession zooKeeperSession = ZooKeeperSession.getInstance();
        final CacheService cacheService = (CacheService) SpringContext.getApplicationContext().getBean("cacheService");
        // 持续消耗rebuildCacheQueue中的缓存信息
        while (true) {
            final ProductInfo productInfo = rebuildCacheQueue.getProductInfo();
            // 尝试获取分布式锁,下面要获取redis中该商品信息，巫妖提前上锁，避免读出来的信息不是最新的
            zooKeeperSession.acquireDistributeLock(productInfo.getId());
            final ProductInfo productInfoFromRedisCache = cacheService.getProductInfoFromRedisCache(productInfo.getId());
            if (productInfoFromRedisCache != null) {
                final LocalDateTime productModifyTime = LocalDateTime.parse(productInfo.getModifyTime(), dateTimeFormatter);
                final LocalDateTime productFromRedisModifyTime = LocalDateTime.parse(productInfoFromRedisCache.getModifyTime(), dateTimeFormatter);
                if (!productModifyTime.isAfter(productFromRedisModifyTime)) {
                    // queue中的modifyTime在redis中数据之前，那么表明这个数据不是最新的，可以直接跳过不处理
                    System.out.println("当前product是历史版本，不是最新的，可以跳过不处理。" + "productModifyTime=" + productInfo.getModifyTime());
                    continue;
                }
            } else {
                System.out.println("redis中并无productId=" + productInfo.getId() + "的相关数据");
            }
            System.out.println("当前product需要更新到redis和本地缓存");
            cacheService.saveProductInfo2LocalCache(productInfo);
            cacheService.saveProductInfo2ReidsCache(productInfo);
            // 释放zooKeeper分布式锁
            zooKeeperSession.releaseDistributeLock(productInfo.getId());
        }
    }
}

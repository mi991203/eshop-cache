package com.roncoo.eshop.cache.rebuild;

import com.roncoo.eshop.cache.model.ProductInfo;

import java.util.concurrent.ArrayBlockingQueue;

public class RebuildCacheQueue {
    private ArrayBlockingQueue<ProductInfo> arrayBlockingQueue = new ArrayBlockingQueue<>(1000);

    public void putProductInfo(ProductInfo productInfo) {
        try {
            arrayBlockingQueue.put(productInfo);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public ProductInfo getProductInfo() {
        try {
            return arrayBlockingQueue.take();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return null;
    }

    // 单例
    private static class Singleton {
        private static RebuildCacheQueue instance;

        static {
            instance = new RebuildCacheQueue();
        }

        public static RebuildCacheQueue getInstance() {
            return instance;
        }
    }

    public static RebuildCacheQueue getInstance() {
        return Singleton.getInstance();
    }

    public static void init() {
        RebuildCacheQueue.getInstance();
    }

}

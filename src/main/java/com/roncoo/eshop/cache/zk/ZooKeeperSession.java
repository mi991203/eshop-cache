package com.roncoo.eshop.cache.zk;

import org.apache.zookeeper.*;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * ZooKeeper分布式锁
 */
public class ZooKeeperSession {
    private final CountDownLatch countDownLatch = new CountDownLatch(1);
    private ZooKeeper zooKeeper;

    public ZooKeeperSession() {
        try {
            this.zooKeeper = new ZooKeeper("192.168.137.10:2181,192.168.137.20:2181,192.168.137.30:2181", 20000, new ZooKeeperWatcher());
            System.out.println("zookeeper session state: " + zooKeeper.getState());
            // 计数器阻塞等待，等待计数为0
            // 默认获取ZookeeperSession连接是异步的
            countDownLatch.await();
            // 成功获取连接，打印状态
            System.out.println("zookeeper session state: " + zooKeeper.getState());
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取分布式锁
     * @param productId 产品Id
     */
    public void acquireDistributeLock(Long productId) {
        final String path = "/product-lock-" + productId;
        try {
            zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
            // 获取锁失败，多次尝试
            while (true) {
                try {
                    Thread.sleep(500);
                    zooKeeper.create(path, "".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
                } catch (InterruptedException | KeeperException ex) {
                    ex.printStackTrace();
                    continue;
                }
                break;
            }
        }
    }

    /**
     * 释放分布式锁
     * @param productId 产品Id
     */
    public void releaseDistributeLock(Long productId) {
        final String path = "/product-lock-" + productId;
        try {
            zooKeeper.delete(path, -1);
        } catch (InterruptedException | KeeperException e) {
            e.printStackTrace();
        }
    }

    private class ZooKeeperWatcher implements Watcher {

        @Override
        public void process(WatchedEvent watchedEvent) {
            System.out.println("receive watch event" + watchedEvent.getState());
            if (Event.KeeperState.SyncConnected == watchedEvent.getState()) {
                countDownLatch.countDown();
            }
        }
    }

    /*
    单例内部类获取zookeeperSession节点信息
     */
    private static class Singleton {
        private static ZooKeeperSession instance;

        static {
            instance = new ZooKeeperSession();
        }

        public static ZooKeeperSession getInstance() {
            return instance;
        }
    }

    public static ZooKeeperSession getInstance() {
        return Singleton.getInstance();
    }

    public static void init() {
        getInstance();
    }
}

package com.roncoo.eshop.cache.listener;

import com.roncoo.eshop.cache.kafka.KafkaConsumer;
import com.roncoo.eshop.cache.rebuild.RebuildCacheQueue;
import com.roncoo.eshop.cache.rebuild.RebuildCacheThread;
import com.roncoo.eshop.cache.spring.SpringContext;
import com.roncoo.eshop.cache.zk.ZooKeeperSession;
import org.springframework.context.ApplicationContext;
import org.springframework.web.context.support.WebApplicationContextUtils;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class InitListener implements ServletContextListener {
    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        ServletContext sc = servletContextEvent.getServletContext();
        ApplicationContext context = WebApplicationContextUtils.getWebApplicationContext(sc);
        SpringContext.setApplicationContext(context);

        RebuildCacheQueue.init();
        ZooKeeperSession.init();

        new Thread(new KafkaConsumer("cache-message")).start();
        new Thread(new RebuildCacheThread()).start();
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {

    }
}

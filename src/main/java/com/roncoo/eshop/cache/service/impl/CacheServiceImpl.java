package com.roncoo.eshop.cache.service.impl;

import com.alibaba.fastjson.JSONObject;
import com.roncoo.eshop.cache.model.ShopInfo;
import com.roncoo.eshop.cache.service.CacheService;
import org.springframework.cache.annotation.CachePut;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import com.roncoo.eshop.cache.model.ProductInfo;
import redis.clients.jedis.JedisCluster;

import javax.annotation.Resource;

/**
 * 缓存Service实现类
 * @author Administrator
 *
 */
@Service("cacheService")
public class CacheServiceImpl implements CacheService {
	
	public static final String CACHE_NAME = "local";

	@Resource
	private JedisCluster jedisCluster;

	@CachePut(value = CACHE_NAME, key = "'key_'+#productInfo.getId()")
	public ProductInfo saveLocalCache(ProductInfo productInfo) {
		return productInfo;
	}

	@Cacheable(value = CACHE_NAME, key = "'key_'+#id")
	public ProductInfo getLocalCache(Long id) {
		return null;
	}

	@CachePut(value = CACHE_NAME, key = "'product_info_'+#productInfo.getId()")
	public ProductInfo saveProductInfo2LocalCache(ProductInfo productInfo) {
		return productInfo;
	}

	@Cacheable(value = CACHE_NAME, key = "'product_info_'+#productId")
	public ProductInfo getProductInfoFromLocalCache(Long productId) {
		return null;
	}

	@CachePut(value = CACHE_NAME, key = "'shop_info_'+#shopInfo.getId()")
	public ShopInfo saveShopInfo2LocalCache(ShopInfo shopInfo) {
		return shopInfo;
	}

	@Cacheable(value = CACHE_NAME, key = "'shop_info_'+#shopId")
	public ShopInfo getShopInfoFromLocalCache(Long shopId) {
		return null;
	}

	public void saveProductInfo2ReidsCache(ProductInfo productInfo) {
		String key = "product_info_" + productInfo.getId();
		jedisCluster.set(key, JSONObject.toJSONString(productInfo));
	}

	public void saveShopInfo2ReidsCache(ShopInfo shopInfo) {
		String key = "shop_info_" + shopInfo.getId();
		jedisCluster.set(key, JSONObject.toJSONString(shopInfo));
	}

	/**
	 * 从redis中获取商品信息
	 * @param productId
	 */
	public ProductInfo getProductInfoFromRedisCache(Long productId) {
		String key = "product_info_" + productId;
		String json = jedisCluster.get(key);
		return JSONObject.parseObject(json, ProductInfo.class);
	}

	/**
	 * 从redis中获取店铺信息
	 * @param shopId
	 */
	public ShopInfo getShopInfoFromReidsCache(Long shopId) {
		String key = "shop_info_" + shopId;
		String json = jedisCluster.get(key);
		return JSONObject.parseObject(json, ShopInfo.class);
	}
	
}

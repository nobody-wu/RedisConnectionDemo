package top.bigpong.dao;

import com.alibaba.fastjson.JSON;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.JedisCluster;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Cornelius on 2017/6/28.
 */
public class JedisHelper {
	
	protected transient final Logger logger = LoggerFactory.getLogger(getClass());
	
	/**
	 * 操作域
	 */
	private String domain;
	
	/**
	 * redis集群操作
	 */
	public JedisCluster jedisCluster;
	
	/**
	 * 读数据加域
	 *
	 * @param key
	 * @return
	 */
	public <T> T get(final String key, Type type) {
		final String fullkey = domain + ":" + key;
		return getB(fullkey, type);
	}
	
	/**
	 * 读数据
	 *
	 * @return
	 */
	public <T> T getB(final String fullkey,Type type) {
		
		// 得到value
		if (jedisCluster.exists(fullkey)) {
			String reslut = jedisCluster.get(fullkey);
			return JSON.parseObject(reslut, type);
		} else {
			return null;
		}
	}
	
	/**
	 * map 取值 加域
	 *
	 * @param key
	 * @param field
	 * @return
	 */
	public <T> T hget(final String key, final String field,Type type) {
		String fullkey = domain + ":" + key;
		return hgetB(fullkey, field, type);
	}
	
	/**
	 * map 取值
	 *
	 * @param fullkey
	 * @param field
	 * @return
	 */
	public <T> T hgetB(final String fullkey, final String field,Type type) {
		
		// 得到value
		if (jedisCluster.exists(fullkey)) {
			String result = jedisCluster.hget(fullkey, field);
			return JSON.parseObject(result, type);
		} else {
			return null;
		}
		
	}
	
	/**
	 * 删除数据
	 * 返回值为：被删除 key 的数量
	 * @param key
	 */
	public Long hdel(final String key, final String field) {
		final String fullkey = domain + ":" + key;
		return hdelb(fullkey, field);
	}
	
	/**
	 * 删除原始键值
	 *
	 */
	public Long hdelb(final String fullkey, final String field) {
		
		Long delNum = 0L;
		if (jedisCluster.exists(fullkey)) {
			delNum = jedisCluster.del(fullkey);
		}
		
		return delNum;
	}
	
	/**
	 * map 赋值 加域
	 * 返回值 如果 field 是哈希表中的一个新建域，并且值设置成功，返回 1 。
	 *     如果哈希表中域 field 已经存在且旧值已被新值覆盖，返回 0 。
	 * @param key
	 * @param field
	 * @param value
	 */
	public Long hset(final String key, final String field, Object value) {
		String fullkey = domain + ":" + key;
		return hsetb(fullkey, field, JSON.toJSONString(value));
	}
	
	/**
	 * map 赋值
	 *
	 * @param fullkey
	 * @param field
	 * @param value
	 * @return
	 */
	public Long hsetb(final String fullkey, final String field, final String value) {
		
		return jedisCluster.hset(fullkey, field, value);
		
	}
	
	/**
	 * 取map 加域
	 *
	 * @param key
	 * @return
	 */
	public Map<String, String> hgetMap(final String key) {
		String fullkey = domain + ":" + key;
		return hgetMapB(fullkey);
	}
	
	/**
	 * 取map
	 *
	 * @param fullkey
	 * @return
	 */
	public Map<String, String> hgetMapB(final String fullkey) {
		Map<String, String> result = new HashMap<String, String>();
		result = jedisCluster.hgetAll(fullkey);
		return result;
	}
	
	/**
	 * 写数据加域
	 * SET 在设置操作成功完成时，返回 OK 。
	 * 如果设置了 NX 或者 XX ，但因为条件没达到而造成设置操作未执行，那么命令返回空批量回复（NULL Bulk Reply）。
	 * @param key
	 * @param value
	 */
	public String set(final String key, Object value) {
		final String fullkey = domain + ":" + key;
		return this.setB(fullkey, JSON.toJSONString(value));
	}
	
	/**
	 * 写数据
	 *
	 * @param value
	 */
	public String setB(final String fullkey, final String value) {
		return  jedisCluster.set(fullkey, value);
	}
	
	
	/**
	 * 删除数据
	 * 返回值：被删除 key 的数量
	 * @param key
	 */
	public Long del(final String key) {
		final String fullkey = domain + ":" + key;
		return delB(fullkey);
	}
	
	/**
	 * 删除原始键值
	 *
	 */
	public Long delB(final String fullkey) {
		return jedisCluster.del(fullkey);
	}
	
	/**
	 * 设置过期加域
	 *
	 * @param key
	 * @param time
	 */
	public Long expire(final String key, final int time) {
		final String fullkey = domain + ":" + key;
		return expireB(fullkey, time);
	}
	
	/**
	 * 设置过期
	 * 返回值：设置成功返回 1 。
	 当 key 不存在或者不能为 key 设置生存时间时，返回 0 。
	 * @param time
	 */
	public Long expireB(final String fullkey, final int time) {
		return jedisCluster.expire(fullkey, time);
	}
	
//	/**
//	 * 得到查找键
//	 *
//	 * @param query
//	 * @return
//	 */
//	public HashSet<String> find(final String query) {
//		final String fullQuery = domain + ":" + query;
//		return findB(fullQuery);
//	}
	
	/**
	 * 得到查找键
	 *
	 * @param query
	 * @return keys
	 */
//	public HashSet<String> findB(final String fullQuery) {
//
//		HashSet<String> keys = new HashSet<String>();
//		Map<String, JedisPool> clusterNodes = jedisCluster.getClusterNodes();
//
//		for (String k : clusterNodes.keySet()) {
//			JedisPool jp = clusterNodes.get(k);
//			Jedis connection = jp.getResource();
//			try {
//				keys.addAll(connection.keys(fullQuery));
//			} catch (Exception e) {
//				logger.error("redis find error", e);
//			} finally {
//				connection.close();
//			}
//		}
//
//		return keys;
//	}
	
	/**
	 * 以秒为单位，返回给定 key 的剩余生存时间(TTL, time to live)。
	 * 时间复杂度：O(1)
	 * 返回值：
	 * 当 key 不存在时，返回 -2 。
	 * 当 key 存在但没有设置剩余生存时间时，返回 -1 。
	 * 否则，以秒为单位，返回 key 的剩余生存时间。
	 *
	 * @param key
	 * @return
	 */
	public Long ttl(final String key) {
		final String fullQuery = domain + ":" + key;
		return ttlb(fullQuery);
	}
	
	/**
	 * 以秒为单位，返回给定 key 的剩余生存时间(TTL, time to live)。
	 * 时间复杂度：O(1)
	 * 返回值：
	 * 当 key 不存在时，返回 -2 。
	 * 当 key 存在但没有设置剩余生存时间时，返回 -1 。
	 * 否则，以秒为单位，返回 key 的剩余生存时间。
	 *
	 * @return
	 */
	public Long ttlb(final String fullkey) {
		return jedisCluster.ttl(fullkey);
	}
	
//	/**
//	 * 清除域下所有信息
//	 * 返回值：删除个数
//	 */
//	public Integer clear() {
//		final String key = "*";
//		HashSet<String> keys = find(key);
//		for (String ki : keys) {
//			delB(ki);
//		}
//		return keys.size();
//	}
	
	
	/**
	 * get/set
	 */
	public String getDomain() {
		return domain;
	}
	
	public void setDomain(String domain) {
		this.domain = domain;
	}
	
	public JedisCluster getJedisCluster() {
		return jedisCluster;
	}
	
	public void setJedisCluster(JedisCluster jedisCluster) {
		this.jedisCluster = jedisCluster;
	}
	
	
}

package top.bigpong.dao;

import org.springframework.dao.DataAccessException;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.util.SerializationUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Cornelius on 2017/6/27.
 */
public class RedisHelper {
	
	/**
	 * redis模版工具
	 */
	private RedisTemplate<Serializable, Serializable> redisTemplate;
	/**
	 * 操作域
	 */
	private String domain;
	
	/**
	 * 读数据加域
	 *
	 * @param key
	 * @return
	 */
	public Object get(final String key) {
		final String fullkey = domain + ":" + key;
		return getB(fullkey);
	}
	
	/**
	 * 读数据
	 *
	 * @param key
	 * @return
	 */
	public Object getB(final String key) {
		// 得以id
		// 得到redis map
		final String fullkey = key;
		Object rs = redisTemplate.execute(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				byte[] keyb = SerializationUtils.serialize(fullkey);
				if (connection.exists(keyb)) {
					byte[] value = connection.get(keyb);
					return SerializationUtils.deserialize(value);
				}
				return null;
			}
		});
		if (rs == null)
			return null;
		else
			return rs;
	}
	
	/**
	 * map 取值 加域
	 *
	 * @param key
	 * @param field
	 * @return
	 */
	public Object hget(final String key, final String field) {
		String fullkey = domain + ":" + key;
		return hgetB(fullkey, field);
	}
	
	/**
	 * map 取值
	 *
	 * @param fullkey
	 * @param field
	 * @return
	 */
	public Object hgetB(final String fullkey, final String field) {
		Object rs = redisTemplate.execute(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				byte[] keyb = SerializationUtils.serialize(fullkey);
				byte[] fieldsb = SerializationUtils.serialize(field);
				if (connection.exists(keyb)) {
					byte[] value = connection.hGet(keyb, fieldsb);
					if (value != null)
						return SerializationUtils.deserialize(value);
				}
				return null;
			}
		});
		if (rs == null)
			return null;
		else
			return rs;
	}
	
	/**
	 * 删除数据
	 *
	 * @param key
	 */
	public void hdel(final String key, final String field) {
		final String fullkey = domain + ":" + key;
		hdelb(fullkey, field);
	}
	
	/**
	 * 删除原始键值
	 *
	 * @param key
	 */
	public void hdelb(final String key, final String field) {
		final String fullkey = key;
		redisTemplate.execute(new RedisCallback<Serializable>() {
			public Serializable doInRedis(RedisConnection connection) throws DataAccessException {
				connection.del(SerializationUtils.serialize(fullkey));
				return null;
			}
		});
	}
	
	/**
	 * map 赋值 加域
	 *
	 * @param key
	 * @param field
	 * @param value
	 */
	public void hset(final String key, final String field, final Serializable value) {
		String fullkey = domain + ":" + key;
		hsetb(fullkey, field, value);
	}
	
	/**
	 * map 赋值
	 *
	 * @param key
	 * @param field
	 * @param value
	 */
	public void hsetb(final String key, final String field, final Serializable value) {
		final String fullkey = key;
		redisTemplate.execute(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				connection.hSet(SerializationUtils.serialize(fullkey), SerializationUtils
						.serialize(field), SerializationUtils.serialize(value));
				return null;
			}
		});
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
		result = redisTemplate.execute(new RedisCallback<Map<String, String>>() {
			public Map<String, String> doInRedis(RedisConnection connection)
					throws DataAccessException {
				byte[] keyb = SerializationUtils.serialize(fullkey);
				Map<String, String> resultM = new HashMap<String, String>();
				if (connection.exists(keyb)) {
					Map<byte[], byte[]> value = connection.hGetAll(keyb);
					for (byte[] mk : value.keySet()) {
						byte[] mv = value.get(mk);
						resultM.put((String) SerializationUtils.deserialize(mk),
								(String) SerializationUtils.deserialize(mv));
					}
					return resultM;
				}
				return null;
			}
		});
		return result;
	}
	
	/**
	 * 写数据加域
	 *
	 * @param key
	 * @param value
	 */
	public void set(final String key, final Serializable value) {
		final String fullkey = domain + ":" + key;
		this.setB(fullkey, value);
	}
	
	/**
	 * 写数据
	 *
	 * @param key
	 * @param value
	 */
	public void setB(final String key, final Serializable value) {
		final String fullkey = key;
		redisTemplate.execute(new RedisCallback<Object>() {
			public Object doInRedis(RedisConnection connection) throws DataAccessException {
				connection.set(SerializationUtils.serialize(fullkey), SerializationUtils
						.serialize(value));
				return null;
			}
		});
	}
	
	/**
	 * 删除数据
	 *
	 * @param key
	 */
	public void del(final String key) {
		final String fullkey = domain + ":" + key;
		delB(fullkey);
	}
	
	/**
	 * 删除原始键值
	 *
	 * @param key
	 */
	public void delB(final String key) {
		final String fullkey = key;
		redisTemplate.execute(new RedisCallback<Serializable>() {
			public Serializable doInRedis(RedisConnection connection) throws DataAccessException {
				connection.del(SerializationUtils.serialize(fullkey));
				return null;
			}
		});
	}
	
	/**
	 * 设置过期加域
	 *
	 * @param key
	 * @param time
	 */
	public void expire(final String key, final long time) {
		final String fullkey = domain + ":" + key;
		expireB(fullkey, time);
	}
	
	/**
	 * 设置过期
	 *
	 * @param key
	 * @param time
	 */
	public void expireB(final String key, final long time) {
		final String fullkey = key;
		redisTemplate.execute(new RedisCallback<Serializable>() {
			public Serializable doInRedis(RedisConnection connection) throws DataAccessException {
				connection.expire(SerializationUtils.serialize(fullkey), time);
				return null;
			}
		});
	}
	
	/**
	 * 得到查找键
	 *
	 * @param query
	 * @return
	 */
	public List<String> find(final String query) {
		final String fullQuery = domain + ":" + query;
		return findB(fullQuery);
	}
	
	/**
	 * 得到查找键
	 *
	 * @param query
	 * @return keys
	 */
	public List<String> findB(final String query) {
		final String fullQuery = query;
		Set<byte[]> resultB = redisTemplate.execute(new RedisCallback<Set<byte[]>>() {
			public Set<byte[]> doInRedis(RedisConnection connection) throws DataAccessException {
				return connection.keys(SerializationUtils.serialize(fullQuery));
			}
		});
		List<String> result = new ArrayList<String>();
		for (byte[] i : resultB) {
			result.add((String) SerializationUtils.deserialize(i));
		}
		return result;
	}
	
	/**
	 * 清除域下所有信息
	 */
	public void clear() {
		final String key = "*";
		List<String> keys = find(key);
		for (String ki : keys) {
			delB(ki);
		}
	}
	
	public void setRedisTemplate(RedisTemplate<Serializable, Serializable> redisTemplate) {
		this.redisTemplate = redisTemplate;
	}
	
	public String getDomain() {
		return domain;
	}
	
	public void setDomain(String domain) {
		this.domain = domain;
	}
	
	
	
}

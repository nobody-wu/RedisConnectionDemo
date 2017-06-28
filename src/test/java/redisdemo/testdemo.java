package redisdemo;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import top.bigpong.dao.JedisHelper;
import top.bigpong.dao.RedisHelper;

import javax.annotation.Resource;

/**
 * Created by Cornelius on 2017/6/27.
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:spring-config-redis.xml")
public class testdemo {

	@Resource
	public RedisHelper redisHelper;
	
	@Resource
	public JedisHelper jedisHelper;
	
	@Test
	public void test(){
		System.out.print(redisHelper.getB("name"));
	}
	
	@Test
	public void test1(){
		System.out.println(jedisHelper.getB("name", String.class));
	}
	
}

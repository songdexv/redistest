package com.songdexv.redis;


import org.junit.BeforeClass;
import org.junit.Test;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Created by Administrator on 2018/4/14.
 */
public class SentinelTest {
    static JedisPoolConfig poolConfig = new JedisPoolConfig();

    @BeforeClass
    public static void init() {
        poolConfig.setMaxTotal(30);
        poolConfig.setMaxIdle(20);
        poolConfig.setMinIdle(5);
        poolConfig.setMaxWaitMillis(6000);
        poolConfig.setTestOnBorrow(true);
    }

    @Test
    public void test() throws InterruptedException {
        String masterName = "mymaster";
        Set<String> sentinels = new HashSet<String>();
        sentinels.add("192.168.1.1:8479");
        sentinels.add("192.168.1.2:8479");
        sentinels.add("192.168.1.3:8479");
        JedisSentinelPool jedisSentinelPool = new JedisSentinelPool(masterName, sentinels, poolConfig, 5000, "passwd");
        HostAndPort currentMaster = jedisSentinelPool.getCurrentHostMaster();
        System.out.println("current master: " + currentMaster.getHost() + ":" + currentMaster.getPort());
        Jedis resouce = jedisSentinelPool.getResource();
        String result = "";
        boolean connected = false;
        for (int i = 0; i < 100; i++) {
            try {
                result = resouce.set("test-" + i, "test value " + i);
                System.out.println(i + ": " + result);
                TimeUnit.MILLISECONDS.sleep(100);
//            resouce.del("test-" + i);
            } catch (JedisConnectionException e) {
                while (!connected) {
                    Thread.sleep(2000);
                    try {
                        resouce = jedisSentinelPool.getResource();
                        connected = true;
                    } catch (Exception e1) {
                        e1.printStackTrace();
                    }
                }
            }
        }
        resouce.close();

    }

    @Test
    public void testShard() throws InterruptedException {
        List<String> masterNames = new ArrayList<String>();
        masterNames.add("mymaster");
        masterNames.add("mymaster2");
        Set<String> sentinels = new HashSet<String>();
        sentinels.add("192.168.1.1:8479");
        sentinels.add("192.168.1.2:8479");
        sentinels.add("192.168.1.3:8479");
        ShardedJedisSentinelPool pool = new ShardedJedisSentinelPool(masterNames, sentinels, "passwd");
        ShardedJedis resouce = pool.getResource();
        String result;
        boolean connected = false;
        for (int i = 0; i < 20; i++) {
            try {
                String key = "shard-test-" + i;
//                System.out.println(resouce.getShard(key));;
//                result = resouce.set("shard-test-" + i, "shard test value " + i);
//                System.out.println(i + ": " + result);
//                TimeUnit.MILLISECONDS.sleep(1000);
//            resouce.del("test-" + i);
                System.out.println(resouce.get(key));
            } catch (JedisConnectionException e) {
                while (!connected) {
                    Thread.sleep(2000);
                    try {
                        resouce = pool.getResource();
                        connected = true;
                    } catch (Exception e1) {
                        e1.printStackTrace();
                    }
                }
            }
        }
        resouce.close();
    }
}

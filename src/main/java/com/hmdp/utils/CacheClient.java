package com.hmdp.utils;

import cn.hutool.core.lang.func.Func;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;

    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    // 解决缓存击穿
    public void setWithLogical(String key, Object value, Long time, TimeUnit unit) {
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));

        // 写入redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    // 为解决缓存穿透，使用向缓存写入null方案
    // 定义类型，然后才能返回类型
    // 不知道类型R是啥，函数中序列化的时候需要用，所以在形参中需要告诉我类型是什么
    public <R, ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        // 1 从redis查询商户缓存
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2 查询是否存在，是否命中
        if (StrUtil.isNotBlank(json)) { // 空字符串，null，换行等都是false
            // 3 存在，直接返回
            return JSONUtil.toBean(json, type);
        }

        // 解决缓存穿透，将空值（""而不是null）写入，所以判断命中的是否是空值，即空字符串
        if (null != json) {  // 此处逻辑是前面isNotBlank判断
            return null;
        }

        // 4 不存在，根据ID查询数据库
        R r = dbFallback.apply(id);

        // 5 数据库也不存在，返回错误
        if (null == r) {
            // 为了解决穿透缓存，需要将空值写入
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
//            return Result.fail("店铺不存在!");
            return null;
        }
        // 6 存在，写入redis
        this.set(key, r, time, unit);

        // 返回店铺信息
//        return Result.ok(shop);
        return r;
    }

    // 为解决缓存击穿，使用逻辑过期
    // 热点key一般会提前放入redis
    public <R, ID> R queryWithLogicalExpireById(String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        // 1 从redis查询商户缓存
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);

        // 2 查询是否存在，是否命中
        if (StrUtil.isBlank(json)) {
            // 3 不存在，直接返回，因为一般热点代码，都会提前放入redis中的
            return null;
        }

        // 4 命中，需要判断过期时间，把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        JSONObject data = (JSONObject) redisData.getData();
        R r = JSONUtil.toBean(data, type);
        LocalDateTime expireTime = redisData.getExpireTime();

        // 5 判断是否过期
        if (expireTime.isAfter(LocalDateTime.now())) {
            // 5 1 未过期，直接返回店铺信息
            return r;
        }

        // 5 2 已经过期，需要缓存重建
        // 6 缓存重建
        // 6 1 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);

        // 6 2 判断是否获取锁成功
        if (isLock) {
            // 6 3 成功，开启独立线程，执行重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 重建缓存
                    // 1 查询数据库
                    R r1 = dbFallback.apply(id);
                    System.out.println(r1);
                    // 2 写入redis
                    this.setWithLogical(key, r1, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    // 释放锁
                    unLock(lockKey);
                }
            });
        }

        // 6 4 返回过期的商铺信息，注意，知道过期了，开启了另外一个线程重建，但是这次返回的还是过期状态
        return r;
    }

    // 线程池
    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);



    // 通过不存在才写入，不存在写入成功为1，存在不成功为0
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);

        // flag返回会做拆箱的，拆箱可能为空，如果flag为空的话，拆箱可能为空
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }
}

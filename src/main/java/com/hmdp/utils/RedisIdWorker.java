package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Component
public class RedisIdWorker {
    private StringRedisTemplate stringRedisTemplate;
    /**
     * 开始的时间戳
     * 序列号位数
     */
    private static final long BEGIN_TIMESTAMP = 1640995200;
    private static final int COUNT_BITS = 32;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }



    public long nextId(String keyPrefix) {  // 64位
        // 1 生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timeStamp = nowSecond - BEGIN_TIMESTAMP;

        // 2 生成序列号，自增
        // 2 1 生成日期，精确到天，放到下面的key中，这样的话每一天都是一个新的key，否则会32位满
        String data = now.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
        // 2 2 自增长   若不存在，自动创建+1
        long count = stringRedisTemplate.opsForValue().increment("icr" + keyPrefix + ":" + data);

        // 拼接，返回
        return timeStamp << COUNT_BITS | count;
    }

    public static void main(String[] args) {
        LocalDateTime time = LocalDateTime.of(2022, 1, 1, 0, 0, 0);
        long second = time.toEpochSecond(ZoneOffset.UTC);
        System.out.println("second = " + second);
    }

}

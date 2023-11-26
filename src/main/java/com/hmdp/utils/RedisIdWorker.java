package com.hmdp.utils;


import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;


@Component
public class RedisIdWorker {
    @Resource
    StringRedisTemplate stringRedisTemplate;
    //开始时间戳
    private static final  long BEGIN_TIMESTAMP = 1640995200L;
    private static final int COUNT_BITS = 32;
    //全局唯一id生成器
    public long nextId(String prefix){
        //1.生成时间戳
        long nowTimestamp = LocalDateTime.now().toEpochSecond(ZoneOffset.UTC);
        long timestamp = nowTimestamp - BEGIN_TIMESTAMP;
        //2.生成序列号
        String date = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        String key = "icr:" + prefix + ":" +  date;
        long count = stringRedisTemplate.opsForValue().increment(key);
        //3.拼接时间戳和序列号并返回
        return timestamp << COUNT_BITS | count;
    }
}

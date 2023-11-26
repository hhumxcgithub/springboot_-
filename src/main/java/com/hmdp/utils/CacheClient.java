package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.security.Key;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

@Slf4j
@Component
public class CacheClient {
    //封装缓存操作的类
    @Resource
    StringRedisTemplate stringRedisTemplate;

    //将任意java对象序列化为json,存储到redis,并设置TTL
    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    //将java对象存入string类型key中,并设置逻辑过期时间
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    //根据指定key查询缓存,并反序列化为对象,通过缓存空对象解决缓存穿透
    public <R, ID> R queryWithPassThrough(
            String prefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        //从redis查询key所存储的json
        String key = prefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            //如果json非空,返回数据
            return JSONUtil.toBean(json, type);
        }
        if (json != null) {
            //如果json为空,则说明redis中缓存的是空对象,返回null
            return null;
        }
        //如果redis未命中,查询数据库
        R r = dbFallback.apply(id);
        if (r == null) {
            //数据库未命中,缓存空对象,空对象设置固定过期时间,并返回null
            stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //数据库命中,缓存数据,数据根据参数设置过期时间,并返回数据
        this.set(key, r, time, unit);
        return r;
    }
    //根据指定key查询缓存,将json数据反序列化为对象,通过互斥锁解决缓存击穿
    public <R,ID> R queryWithMutex(
            String prefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        //根据key查询redis
        String key = prefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)){
            //redis命中,json非空,返回数据
            return JSONUtil.toBean(json,type);
        }
        if (json != null){
            //redis命中的数据为空对象,返回null
            return null;
        }
        R r;
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        try {
            //redis未命中,尝试获取锁
            boolean isLock = tryLock(lockKey);
            //获取锁失败则睡眠并重新查询redis
            if (!isLock){
                Thread.sleep(50);
                queryWithMutex(prefix,id,type,dbFallback,time,unit);
            }
            //获取锁成功,查询数据库
            r = dbFallback.apply(id);
            //数据库未命中,将空对象写入redis,并返回null
            if (r == null){
                stringRedisTemplate.opsForValue().set(key, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //数据库命中,将数据写入redis
            this.set(key, r, time, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            //释放锁
            unlock(lockKey);
        }
        //返回数据
        return r;
    }

    private boolean tryLock(String key) {
        Boolean lock = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        //防止出现空指针异常
        return BooleanUtil.isTrue(lock);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
}

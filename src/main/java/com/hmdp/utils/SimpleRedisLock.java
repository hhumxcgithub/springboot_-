package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.BooleanUtil;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.data.redis.core.script.RedisScript;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements ILock{

    private String name;
    private StringRedisTemplate stringRedisTemplate;
    private static final String  KEY_PREFIX = "lock:";
    private static final String  ID_PREFIX = UUID.randomUUID().toString(true) + "-";

    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT;
    static {
        UNLOCK_SCRIPT = new DefaultRedisScript<>();
        UNLOCK_SCRIPT.setLocation(new ClassPathResource("unlock.lua"));
        UNLOCK_SCRIPT.setResultType(Long.class);
    }

    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }

    @Override
    public boolean tryLock(long timeOutSec) {
        //获取线程的唯一标识:uuid-线程id
        String value = ID_PREFIX + Thread.currentThread().getId();
        //获取锁的同时通过value标记锁的持有者
        Boolean success = stringRedisTemplate.
                opsForValue().setIfAbsent(KEY_PREFIX + name, value, timeOutSec, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(success);
    }
    @Override
    public void unlock() {
        //调用lua脚本,实现释放锁的原子操作
        //获取当前线程标识
        String threadId = ID_PREFIX + Thread.currentThread().getId();
        stringRedisTemplate.
                execute(UNLOCK_SCRIPT, Collections.singletonList(KEY_PREFIX + name),threadId);
    }
/*    @Override
    public void unlock() {
        //释放锁之前判断锁是否是自己持有的,防止误删其他线程的锁
        //1.获取当前线程标识
        String value = ID_PREFIX + Thread.currentThread().getId();
        //2.获取锁中的线程标识
        String lockValue = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);
        if (value.equals(lockValue)) {
            //确定是该线程的锁则释放锁
            stringRedisTemplate.delete(KEY_PREFIX + name);
        }
        //若不是该线程的锁,则不作任何操作
    }*/
}

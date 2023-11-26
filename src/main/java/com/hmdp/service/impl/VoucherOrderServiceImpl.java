package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IVoucherService;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    ISeckillVoucherService iSeckillVoucherService;
    @Resource
    RedisIdWorker redisIdWorker;
    @Resource
    StringRedisTemplate stringRedisTemplate;
    IVoucherOrderService proxy;


    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    //创建阻塞队列
    //private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    //创建线程池
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            while (true) {
                try {
                    //从消息队列里获取订单信息xreadgroup group g1 c1 count 1 block 2000 streams stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create("stream.orders", ReadOffset.lastConsumed())
                    );
                    //获取失败,说明没有消息,continue
                    if (list == null || list.isEmpty()){
                        continue;
                    }
                    //获取成功,先解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.
                            fillBeanWithMap(values, new VoucherOrder(), true);
                    //处理订单
                    createVoucherOrderPlus(voucherOrder);
                    //ack确认 sack stream.orders g1 id
                    stringRedisTemplate.opsForStream().
                            acknowledge("stream.orders","g1",record.getId());
                }catch (Exception e){
                    log.error("处理订单异常",e);
                    handlePendingList();

                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    //从pendinglist里获取订单信息xreadgroup group g1 c1 count 1  streams stream.orders 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create("stream.orders", ReadOffset.from("0"))
                    );
                    //获取失败,说明pending-list没有消息,退出循环
                    if (list == null || list.isEmpty()){
                        break;
                    }
                    //获取成功,先解析消息中的订单信息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.
                            fillBeanWithMap(values, new VoucherOrder(), true);
                    //处理订单
                    createVoucherOrderPlus(voucherOrder);
                    //ack确认 sack stream.orders g1 id
                    stringRedisTemplate.opsForStream().
                            acknowledge("stream.orders","g1",record.getId());
                }catch (Exception e){
                    log.error("处理pending-list订单异常",e);
                }
            }
        }
    }

    @Transactional
    public void createVoucherOrderPlus(VoucherOrder voucherOrder) {
        Long voucherId = voucherOrder.getVoucherId();
        //扣减库存
        boolean success = iSeckillVoucherService.
                update().setSql("stock = stock - 1").
                eq("voucher_id", voucherId).gt("stock", 0).update();
        if (!success) {
            log.error("库存不足");
        }
        //将订单写入数据库
        save(voucherOrder);
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long orderId = redisIdWorker.nextId("order");
        //执行lua脚本判断秒杀资格以及发送订单消息
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT, Collections.emptyList(),
                UserHolder.getUser().getId().toString(), voucherId.toString(),
                orderId.toString()
        );
        int r = result.intValue();
        if (r != 0) {
            return Result.fail(result == 1 ? "库存不足!" : "无法重复下单!");
        }
        //初始化代理对象
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //返回
        return Result.ok(orderId);

    }
    /*@Override
    public Result seckillVoucher(Long voucherId) {
        //1.根据优惠券id查询优惠券,需要拿到优惠券的service才能对优惠券进行查询
        SeckillVoucher seckillVoucher = iSeckillVoucherService.query().eq("voucher_Id", voucherId).one();
        //2.判断优惠券秒杀是否开始
        if (seckillVoucher.getBeginTime().isAfter(LocalDateTime.now())) {
            return Result.fail("活动尚未开始");
        }
        //3.判断秒杀是否结束
        if (seckillVoucher.getEndTime().isBefore(LocalDateTime.now())) {
            return Result.fail("活动已经结束");
        }
        //4.判断库存是否充足
        if (seckillVoucher.getStock() < 1) {
            return Result.fail("库存不足");
        }

        //5.一人一单,防止黄牛
        //5.1获取用户id
        Long userId = UserHolder.getUser().getId();

        //需要将整个方法上锁,因为事务在函数执行完才会提交,锁需要在事务提交完才释放
        SimpleRedisLock simpleRedisLock = new SimpleRedisLock("order" + userId, stringRedisTemplate);
        boolean isLock = simpleRedisLock.tryLock(10);
        if (!isLock){
            //获取锁失败,返回提示信息
            return Result.fail("无法重复下单~");
        }
        try {
            //这里直接调用方法是this调用的方法,不具有事务
            //需要获取代理对象,确保事务生效
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId, userId);
        } finally {
            //释放锁
            simpleRedisLock.unlock();
        }
    }*/

    @Transactional
    public Result createVoucherOrder(Long voucherId, Long userId) {


        //5.2从数据库查询是否已经存在该用户id下的次优惠券的订单
        Integer count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            //如果已经存在,则无法下单
            return Result.fail("无法多次下单哦~");
        }
        //6.扣减库存
        boolean success = iSeckillVoucherService.
                update().setSql("stock = stock - 1").
                eq("voucher_id", voucherId).gt("stock", 0).update();
        if (!success) {
            return Result.fail("库存不足");
        }
        //7.创建订单写入数据库 :订单id号,用户id,购买的代金券id
        VoucherOrder voucherOrder = new VoucherOrder();
        //7.1订单id,全局唯一id生成器
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //7.2用户id,threadlocal中存储着用户信息,上面已经拿到了
        voucherOrder.setUserId(userId);
        //7.3代金券id
        voucherOrder.setVoucherId(voucherId);
        //7.4写入数据库
        save(voucherOrder);
        //8.返回订单id
        return Result.ok(orderId);
    }
}

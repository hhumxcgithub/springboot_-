package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;

import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    StringRedisTemplate stringRedisTemplate;
    @Resource
    CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存击穿加缓存穿透解决
        //Shop shop = queryWithMutex(id);
        Shop shop = cacheClient.
                queryWithMutex(CACHE_SHOP_KEY, id, Shop.class, id2 -> query().eq("id", id2).one(), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        if (shop == null) {
            return Result.fail("商户信息不存在");
        }
        return Result.ok(shop);
    }

    @Override
    @Transactional//添加事务保证对数据操作的原子性
    public Result updateShop(Shop shop) {
        //在更新数据库商户数据的同时,要保证redis缓存中的数据被删除,从而保证数据的一致性
        Long id = shop.getId();
        if (id == null) {
            //id为空则返回错误
            return Result.fail("店铺id不能为空!");
        }
        //1.更新数据库
        updateById(shop);
        //2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        //判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 根据类型分页查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }
        //计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;
        //查询redis,按照距离排序
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo().search(key,
                GeoReference.fromCoordinate(x, y),
                new Distance(5000),
                RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
        );
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            //如果没有下一页的数据了,返回空
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            String idStr = result.getContent().getName();
            Distance distance = result.getDistance();
            ids.add(Long.valueOf(idStr));
            distanceMap.put(idStr, distance);
        });
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("order by field (id," + idStr + ")").list();
        for (Shop shop : shops) {
            //设置店铺的距离成员变量
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        return Result.ok(shops);
    }

    private boolean tryLock(String key) {
        Boolean lock = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        //防止出现空指针异常
        return BooleanUtil.isTrue(lock);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    private Shop queryWithMutex(Long id) {
        //1.从redis中查询商户信息
        String key = CACHE_SHOP_KEY + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        //2.判断商户是否存在
        if (!StrUtil.isBlank(shopJson)) {
            //3.存在,直接返回
            //需要将json转成对象返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        //如果在redis中命中了空值,直接返回错误结果
        if (shopJson != null) {
            return null;
        }
        //4.redis未命中,从数据库中查询
        //防止缓存击穿,先尝试获取锁
        String lockKey = LOCK_SHOP_KEY + "id";
        Shop shop;
        try {
            boolean isLock = tryLock(lockKey);
            //判断是否获取成功
            if (!isLock) {
                //失败则休眠,然后继续尝试去redis获取数据
                Thread.sleep(50);
                //递归
                return queryWithMutex(id);
            }
            //成功,进行数据库查询
            shop = query().eq("id", id).one();
            //5.数据库中也不存在,返回错误
            if (shop == null) {
                //防止出现缓存穿透,在redis中存入一个空值,下次根据该id查询时,请求就不会到达数据库,而是在查询redis后返回
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //6.数据库存在,将数据写入redis并返回
            //6.1将数据写入redis
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);

        } catch (InterruptedException e) {
            throw new RuntimeException();
        } finally {
            //数据库操作完毕,释放锁
            unlock(lockKey);
        }
        //6.2返回数据
        return shop;
    }
}

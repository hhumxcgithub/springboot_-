package com.hmdp;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import com.hmdp.utils.RedisIdWorker;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.geo.Point;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = HmDianPingApplication.class)
public class HmDianPingApplicationTests {
    @Resource
    RedisIdWorker redisIdWorker;
    @Resource
    IShopService shopService;
    @Resource
    StringRedisTemplate stringRedisTemplate;
    private  ExecutorService es = Executors.newFixedThreadPool(500);

    @Test
    public void testRedisIdWorker() {
        Runnable task = () -> {
            for (int i = 0; i < 100; i++) {
                long id = redisIdWorker.nextId("order");
                System.out.println("id = " + id);
            }
        };
        for (int i = 0; i < 300; i++) {
            es.submit(task);
        }
    }

    @Test
    public void loadShopData(){
        //将店铺信息根据不同的类型存入redis
        //geo:typeid  shopid  score
        //1.查询店铺信息
        List<Shop> list = shopService.list();
        //2.将店铺信息通过类型分组
        Map<Long, List<Shop>> map = list.stream().collect(Collectors.groupingBy(Shop::getTypeId));
        //3.将每组信息保存到redis
        for (Map.Entry<Long, List<Shop>> entry : map.entrySet()) {
            Long typeId = entry.getKey();
            List<Shop> shops = entry.getValue();
            String key = "shop:geo:" + typeId;
            for (Shop shop : shops) {
                stringRedisTemplate
                        .opsForGeo().add(key,new Point(shop.getX(),shop.getY()),shop.getId().toString());
            }
        }
    }
}

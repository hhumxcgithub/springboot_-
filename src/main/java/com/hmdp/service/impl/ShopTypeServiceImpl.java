package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.mapper.ShopTypeMapper;
import com.hmdp.service.IShopTypeService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopTypeServiceImpl extends ServiceImpl<ShopTypeMapper, ShopType> implements IShopTypeService {

    @Resource
    StringRedisTemplate stringRedisTemplate;
    @Override
    public Result queryTypeList() {
        String key = "cache:shoptype";
        //1.从redis中查询商户类型信息
        String shopTypeJson = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(shopTypeJson)) {
            //2.redis中存在,返回
            //需要将redis中存储的json转成list返回
            return Result.ok(JSONUtil.toList(shopTypeJson,ShopType.class));
        }

        //3.redis中不存在,去数据库中查询
        List<ShopType> shopTypeList = query().orderByAsc("sort").list();
        //4.数据库中没有商户类型信息,返回错误
        if (shopTypeList == null || shopTypeList.isEmpty()){
            return Result.fail("没有商户类型信息");
        }
        //5.数据库中存在,将商户类型信息保存到redis并返回
        //5.1保存到redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shopTypeList),30, TimeUnit.MINUTES);
        //5.2返回数据
        return Result.ok(shopTypeList);
    }

}

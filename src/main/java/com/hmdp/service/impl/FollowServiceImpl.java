package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.entity.User;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {

    @Resource
    StringRedisTemplate stringRedisTemplate;
    @Resource
    IUserService userService;
    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        Long userId = UserHolder.getUser().getId();
        String key ="follows:" + userId;
        if (isFollow){
            //关注用户,创建关注表,保存到数据库
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            boolean isSuccess = save(follow);
            if (isSuccess) {
                //redis中维护一个set,保存当前id用户关注的用户
                stringRedisTemplate.opsForSet().add(key,followUserId.toString());
            }
        }else {
            //取消关注,删除对应数据库表
            boolean isSuccess = remove(new QueryWrapper<Follow>()
                    .eq("user_id", userId).eq("follow_user_id", followUserId));
            if (isSuccess) {
                stringRedisTemplate.opsForSet().remove(key,followUserId.toString());
            }
        }
        return Result.ok();

    }

    @Override
    public Result isFollow(Long followUserId) {
        //查询数据库
        Long userId = UserHolder.getUser().getId();
        Integer count = query()
                .eq("user_id", userId).eq("follow_user_id", followUserId).count();
        return Result.ok(count > 0);
    }

    @Override
    public Result followCommons(Long id) {
        Long userId = UserHolder.getUser().getId();
        String keyPrefix = "follows:";
        //求交集
        Set<String> intersect = stringRedisTemplate
                .opsForSet().intersect(keyPrefix + userId, keyPrefix + id);
        if (intersect == null || intersect.isEmpty()){
            //如果没有交集,返回空
            return Result.ok(Collections.emptyList());
        }
        //集合不空,解析
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        //查询数据库得到ids对应的users
        //users转成userdtos
        List<UserDTO> userDTOs = userService.listByIds(ids)
                .stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());
        return Result.ok(userDTOs);

    }
}

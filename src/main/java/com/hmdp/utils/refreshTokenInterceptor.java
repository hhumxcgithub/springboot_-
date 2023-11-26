package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.StrUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.LOGIN_USER_KEY;
import static com.hmdp.utils.RedisConstants.LOGIN_USER_TTL;

public class refreshTokenInterceptor implements HandlerInterceptor {

    StringRedisTemplate stringRedisTemplate;

    public refreshTokenInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        //1.获取请求头中的token
        String token = request.getHeader("authorization");
        if (StrUtil.isBlank(token)){
            //token为空则放行,不需要刷新
            return true;
        }
        //2.根据token获得redis中的用户
        Map<Object, Object> userDTOMap = stringRedisTemplate.opsForHash().entries(LOGIN_USER_KEY + token);
        //3.判断用户是否存在
        if (userDTOMap.isEmpty()){
            //4.不存在,放行
            return true;
        }
        //5.将用户信息存放到threadlocal
        //5.1将从redis中查询到的hash数据转为userdto对象
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userDTOMap, new UserDTO(), false);
        //5.2存放userdto到threadlocal
        UserHolder.saveUser(userDTO);
        //6.刷新token有效期
        stringRedisTemplate.expire(LOGIN_USER_KEY + token,LOGIN_USER_TTL, TimeUnit.SECONDS);
        //6.存在,放行
        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        UserHolder.removeUser();
    }
}

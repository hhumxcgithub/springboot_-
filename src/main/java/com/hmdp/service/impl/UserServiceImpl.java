package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.lang.generator.UUIDGenerator;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Formatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
@Slf4j
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Override
    public Result senCode(String phone, HttpSession session) {
        //1.校验手机号
        if (RegexUtils.isPhoneInvalid(phone)){
            //2.手机号错误,返回错误信息
            return Result.fail("手机号格式错误!");
        }
        //3.手机号格式正确,生成随机验证码
        String code = RandomUtil.randomNumbers(6);
        //4.将验证码保存到session中
        //session.setAttribute("code",code);
        //4.将验证码保存到redis
        stringRedisTemplate.opsForValue().set(LOGIN_CODE_KEY + phone,code,LOGIN_CODE_TTL, TimeUnit.MINUTES);
        log.debug("发送成功,验证码: {}",code);
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //1判断手机号格式是否正确
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号格式错误!");
        }
        //2.手机号正确,从session中获取验证码并校验验证码
        String code = loginForm.getCode();
        //String cacheCode = (String) session.getAttribute("code");
        //2.从redis获取验证码并校验
        String cacheCode = stringRedisTemplate.opsForValue().get(LOGIN_CODE_KEY + phone);
        if (cacheCode == null || !cacheCode.equals(code)){
            //3.验证码错误返回错误信息
            return Result.fail("验证码错误!");
        }
        //4.验证码正确,查询用户是否存在
        User user = query().eq("phone", phone).one();
        if (user == null){
            //5.不存在,创建并保存用户
            user = createUserWithPhone(phone);
        }
        //6.将用户信息保存到session
        //session.setAttribute("user",user);
        //6.将用户信息保存到redis
        //6.1生成随机令牌
        String token = UUID.randomUUID().toString(true);
        //6.2将user转成hashmap
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        //这里userdto中有一个long类型的数据,无法直接存到redis中,需要对它做个类型转换
        Map<String, Object> userDTOMap = BeanUtil.beanToMap(userDTO,new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((filedName,filedValue) -> filedValue.toString()));
        //6.3存入redis
        String tokenKey = LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey,userDTOMap);
        //7.设置token有效期,三千六百秒
        stringRedisTemplate.expire(tokenKey,LOGIN_USER_TTL,TimeUnit.SECONDS);
        //8.返回token
        return Result.ok(token);
    }

    @Override
    public Result sign() {
        //获取当前用户
        Long userId = UserHolder.getUser().getId();
        //获取当前日期
        LocalDateTime now = LocalDateTime.now();
        //获取格式化的年月
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        //获取当前是本月第几天
        int dayOfMonth = now.getDayOfMonth();
        //在redis的bitmap签到
        stringRedisTemplate.opsForValue().setBit(key,dayOfMonth - 1,true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        //统计本月最近的连续签到天数
        //获取当前用户
        Long userId = UserHolder.getUser().getId();
        //获取当前日期
        LocalDateTime now = LocalDateTime.now();
        //获取格式化的年月
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key = USER_SIGN_KEY + userId + keySuffix;
        //获取当前是本月第几天
        int dayOfMonth = now.getDayOfMonth();
        //获取本月截止今天的所有签到记录 bitfield key get type offset
        List<Long> result = stringRedisTemplate.opsForValue().bitField(key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0));
        if (result == null || result.isEmpty()){
            return Result.ok(0);
        }
        Long num = result.get(0);
        if (num == null || num == 0){
            return Result.ok(0);
        }
        //遍历bitmap
        int count = 0;
        while (true){
            //与1做与运算得到结果
            long l = num & 1;
            if (l == 0){
                //结果为0,则未签到,结束
                break;
            }else {
                //结果不为0,签到,连续签到计数器加一
                count++;
            }
            //右移一位
            num >>>= 1;
        }
        return Result.ok(count);
    }

    private User createUserWithPhone(String phone) {
        User user = new User();
        user.setPhone(phone);
        user.setNickName("user_" + RandomUtil.randomString(10));
        save(user);
        return user;
    }
}

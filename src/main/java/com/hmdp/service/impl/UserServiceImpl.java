package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
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
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import io.lettuce.core.BitFieldArgs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.servlet.http.HttpSession;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Autowired
    private StringRedisTemplate stringRedisTemplate; // 要求所有属性都是string的

    @Override
    public Result sendCode(String phone, HttpSession session) {
        // 1. 校验手机号
        if (RegexUtils.isPhoneInvalid(phone)) {
            // 2.
            return Result.fail("手机号格式错误");
        }

        // 3. 符合，生成校验码
        String code = RandomUtil.randomNumbers(6);

//        // 4 保存验证码到session
//        session.setAttribute("code", code);
        // 4 保存验证码到redis
        stringRedisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY + phone, code, RedisConstants.LOGIN_CODE_TTL, TimeUnit.MINUTES); // 不加过期的话，一直存，会满

        // 5 发送验证码
        log.debug("发送短信验证码成功，验证码: " + code);

        // 6 返回ok
        return Result.ok();
    }

    @Override
    public Result login(LoginFormDTO loginForm, HttpSession session) {
        // 1 首先还是需要校验手机号，这是两个不同的请求
        // 其实此处应该将手机号码和code对应起来的
        String phone = loginForm.getPhone();
        if (RegexUtils.isPhoneInvalid(phone)) {
            return Result.fail("手机号格式错误");
        }

        // 2 校验验证码
        // cookie会带着一个sessionId，可以得到session
//        Object cacheCode = session.getAttribute("code");
        Object cacheCode = stringRedisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY + phone);
        String code = loginForm.getCode();
        if (null == cacheCode || !cacheCode.toString().equals(code)) { // 反向校验，避免嵌套的
            // 3 不一致，报错
            return Result.fail("验证码错误");
        }

        // 4 一致，根据手机号查询用户  select * from tb_user where phone = ?
        User user = query().eq("phone", phone).one(); // 继承了ServiceImpl<UserMapper, User>

        // 5 判断用户是否存在
        if (null == user) {
            // 6 不存在，自己创建一个
            user = createUserWithPhone(phone);
        }

//        // 7 存入session的不应该是完整信息，因为有些信息太敏感，而且存的越多占内存，虽然越多越方便
//        session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));
        // 7 保存用户信息到cookie
        // 7.1 随机生成toke，作为登录令牌
        String token = UUID.randomUUID().toString();
        // 7.2 将User转为Hash存储
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        // 注意这个类可以好好了解一下，此处因为id本来是long的，但是stringredistemplate需要全部是string
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO, new HashMap<>(),
                CopyOptions.create()
                        .setIgnoreNullValue(true)
                        .setFieldValueEditor((fileName, fieldValue) -> fieldValue.toString()));
        // 存储到redis, 最好一次性putAll，连续存三次交互比较不好，不能一直存着用户，session一般30min，所以这里参考下也用30min
        // 只要30min内不访问，就删除，但是访问的话，就得更新
        String tokenKey = RedisConstants.LOGIN_USER_KEY + token;
        stringRedisTemplate.opsForHash().putAll(tokenKey, userMap);
//        stringRedisTemplate.expire(tokenKey, RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);
        stringRedisTemplate.expire(tokenKey, RedisConstants.LOGIN_USER_TTL, TimeUnit.DAYS);

        // 不需要返回登录凭证，因为每一次cookie都会带一个sessionID，就能找到session // 注意这是之前存入session的
        // 返回token
        return Result.ok(token);
    }

    @Override
    public Result sign() {
        // 1 获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        // 2 获取日期
        LocalDateTime now = LocalDateTime.now();
        // 3 拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyy-MM"));
        String key = RedisConstants.USER_SIGN_KEY + userId + keySuffix;
        // 4 获取今天是本月第几天
        int dayOfMonth = now.getDayOfMonth(); // 是1～31
        // 5 写入redis SETBIT key offset 1
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);

        return Result.ok();
    }

    @Override
    public Result signCount() {
        // 1 获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        // 2 获取日期
        LocalDateTime now = LocalDateTime.now();
        // 3 拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyy-MM"));
        String key = RedisConstants.USER_SIGN_KEY + userId + keySuffix;
        // 4 获取今天是本月第几天
        int dayOfMonth = now.getDayOfMonth(); // 是1～31
        // 5 获取本月截止今天为止的所有的签到记录，返回的十进制数据 BITFIELD jcwang:common:sign:1010:2022-05 u10 0
        List<Long> result = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth)).valueAt(0)
        );
        if (null == result || result.isEmpty()) {
            return Result.ok(0);
        }
        // 这是结果，结果可能也为空
        Long num = result.get(0);
        if (null == num || num == 0) {
            return Result.ok(0);
        }

        // 6 循环遍历
        int count = 0;
        while (true) {
            // 6 1 让这个数字与1做与运算，得到数字的最后一个bit位 // 判断这个bit是否为0
            if ((num & 1) == 0) {
                // 若为0， 未签到，结束
                break;
            } else {
                // 不为0，说明已经签到。计数器+1
                count++;
            }
            // 把数字无符号右移以为，抛弃最后一个bit，继续下一位bit
            num >>>= 1;
        }


        return Result.ok(count);
    }

    private User createUserWithPhone(String phone) {
        // 1 创建用户
        User user = new User();
        user.setPhone(phone);
        user.setNickName(SystemConstants.USER_NICK_NAME_PREFIX + RandomUtil.randomString(10));

        // 2 保存用户
        save(user); // 应该是存到了数据库
        return user;
    }
}

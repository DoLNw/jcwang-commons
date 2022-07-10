package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.UserDTO;
import io.netty.util.internal.StringUtil;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

// 这个类不做拦截，主要是为了刷新token
public class RefreshTokenInterceptor implements HandlerInterceptor {

    // Spring创建的对象（Component这些注解），能够依赖注入，但是手动创建(在WebMvcConfiger中 new)的不行，不能Autowired
    private StringRedisTemplate stringRedisTemplate;

    public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    // 校验
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
//        // 1 获取session
//        HttpSession session = request.getSession();
//        // 2 获取session中的用户
//        UserDTO user = (UserDTO) session.getAttribute("user");
//        // 3 判断用户是否存在
//        if (null == user) {
//            // 4 不存在，拦截，返回401，未授权
//            response.setStatus(401);
//            return false;
//        }


        // 1 获取请求头，里面有token（之前的session只要拿到就好，因为cookie中维护）
        String token = request.getHeader("authorization");
        if (StringUtil.isNullOrEmpty(token)) {
            return true;
        }

        // 2 基于token获得redis中的用户
        String tokenKey = RedisConstants.LOGIN_USER_KEY + token;
        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(tokenKey);

        // 3 判断用户是否存在
        if (userMap.isEmpty()) {
            return true;
        }

        // 5 将查询到的Hash数据转为userDTO对象
        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);

        // 6 存在，保存用户信息到ThreadLocal
        // 已经有了token找user了，这里在存入ThreadLocal中应该只是为了方便如果还是同一个线程的话，重复使用user？
        UserHolder.saveUser(userDTO);

        // 7 刷新token有效期
        // 只要访问过了，就要刷新token，30秒内无操作，才会登录失效
        stringRedisTemplate.expire(tokenKey, RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);

        // 8 放行
        return true;
    }

    // 销毁用户，避免内存泄漏
    // 这个是显示完了执行
    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) throws Exception {
        // 移除用户
        UserHolder.removeUser();
    }
}

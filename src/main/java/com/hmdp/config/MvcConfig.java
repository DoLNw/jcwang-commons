package com.hmdp.config;

import com.hmdp.utils.LoginInterceptor;
import com.hmdp.utils.RefreshTokenInterceptor;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

import javax.annotation.Resource;

// 要想让拦截器生效，需要加这个配置
@Configuration
public class MvcConfig implements WebMvcConfigurer {
    // 这个类是Spring帮我们创建的，所以可以注入
    @Resource
    private StringRedisTemplate stringRedisTemplate;

    // 默认order都是0，那么按照添加顺序执行
    // order越大，执行等级越低
    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        // 不是所有路径都会要拦截
        registry.addInterceptor(new LoginInterceptor())
                .excludePathPatterns(
                        "/user/code",
                        "/user/login",
                        "/blog/hot",
                        "/shop/**",
                        "/shop-type/**",
                        "/upload/**",
                        "/voucher/**"
                ).order(1);

        // 为了访问所有的请求时，都刷新token
        registry.addInterceptor(new RefreshTokenInterceptor(stringRedisTemplate)).addPathPatterns("/**").order(0);
    }
}

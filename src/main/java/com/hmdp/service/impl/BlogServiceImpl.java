package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.Follow;
import com.hmdp.entity.ScrollResult;
import com.hmdp.entity.User;
import com.hmdp.mapper.BlogMapper;
import com.hmdp.service.IBlogService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IFollowService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.ArrayList;
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
public class BlogServiceImpl extends ServiceImpl<BlogMapper, Blog> implements IBlogService {
    
    @Resource
    private IUserService userService;

    @Resource
    private IFollowService followService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;
    
    @Override
    public Result queryHotBlog(Integer current) {
        // 根据用户查询
        Page<Blog> page = query()
                .orderByDesc("liked")
                .page(new Page<>(current, SystemConstants.MAX_PAGE_SIZE));
        // 获取当前页数据
        List<Blog> records = page.getRecords();
        // 查询用户
        // 查询blog是否被点赞了
//        records.forEach(this::queryBlogUser);

        records.forEach(blog -> {
            this.queryBlogUser(blog);
            this.isBlogLiked(blog);
        });

        return Result.ok(records);
    }

    private void queryBlogUser(Blog blog) {
        Long userId = blog.getUserId();
        User user = userService.getById(userId);
        blog.setName(user.getNickName());
        blog.setIcon(user.getIcon());
    }

    @Override
    public Result queryBlogById(Long id) {
        // 1 查询blog
        Blog blog = getById(id);
        if (null == blog) {
            return Result.fail("笔记不存在！");
        }

        // 2 查询bolg有关的用户
        queryBlogUser(blog);

        // 3 查询blog是否被点赞了
        isBlogLiked(blog);

        return Result.ok(blog);
    }

    private void isBlogLiked(Blog blog) {
        // 1 获取登陆用户
        UserDTO user = UserHolder.getUser();
        // 用户未登录，无需查询是否点赞
        if (null == user) {
            return;
        }
        Long userId = user.getId();

        // 2 判断当前用户是否已经点赞
        String key = RedisConstants.BLOG_LIKED_KEY + blog.getId();
//        Boolean isMember = stringRedisTemplate.opsForSet().isMember(key, userId.toString());
//        blog.setIsLike(BooleanUtil.isTrue(isMember));
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
        blog.setIsLike(null != score);
    }

    @Override
    public Result likeBolg(Long id) {
        // 1 获取登陆用户
        Long userId = UserHolder.getUser().getId();
        // 2 判断当前用户是否已经点赞
        String key = RedisConstants.BLOG_LIKED_KEY + id;
//        Boolean isMember = stringRedisTemplate.opsForSet().isMember(key, userId.toString());
        Double score = stringRedisTemplate.opsForZSet().score(key, userId.toString());
//        if (BooleanUtil.isFalse(isMember)) {
        if (null == score) {
            // 3 如果为点赞，可以点赞
            // 3 1 数据库点赞+1
            boolean success = update().setSql("liked = liked + 1").eq("id", id).update();
            // 3 2 保存用户到redis的set集合，表示该blog下该用户已经点赞过
            if (success) {
//                stringRedisTemplate.opsForSet().add(key, userId.toString());
                stringRedisTemplate.opsForZSet().add(key, userId.toString(), System.currentTimeMillis());
            }
        } else {
            // 4 已经点赞，取消点赞
            // 4 2 数据库点赞数-1
            boolean success = update().setSql("liked = liked - 1").eq("id", id).update();
            // 4 3 把用户从redis的set移除
            if (success) {
//                stringRedisTemplate.opsForSet().remove(key, userId.toString());
                stringRedisTemplate.opsForZSet().remove(key, userId.toString());
            }
        }

        return Result.ok();
    }

    @Override
    public Result queryBlogLikesById(Long id) {
        String key = RedisConstants.BLOG_LIKED_KEY + id;
        // 1 查询top5点赞的用户 zrange 0 4，得到的是五个用户的id的string类型的key
        Set<String> top5 = stringRedisTemplate.opsForZSet().range(key, 0, 4);
        if (top5 == null || top5.isEmpty()) {
            return Result.ok(Collections.emptyList());
        }

        // 2 解析出其中的用户id，从set的string专程list的long的
        List<Long> userIds = top5.stream().map(Long::valueOf).collect(Collectors.toList());

        // 3 根据ids返回查询的用户 WHERE id IN (5, 1) ORDER BY FIELD(id, 5, 1)，mysql的in查询出来还是没有顺序的，所以要在加orderby
        String idStr = StrUtil.join(",", userIds);
        List<UserDTO> userDTOS = userService.query().in("id", userIds).last("ORDER BY FIELD (id," + idStr + ")").list()
                .stream()
                .map(user -> BeanUtil.copyProperties(user, UserDTO.class))
                .collect(Collectors.toList());

        // 4 返回
        return Result.ok(userDTOS);
    }

    @Override
    public Result savaBlog(Blog blog) {
        // 1 获取登录用户
        UserDTO user = UserHolder.getUser();
        blog.setUserId(user.getId());
        // 2 保存探店博文
        boolean success = save(blog);
        if (!success) {
            return Result.fail("新增笔记失败！");
        }
        // 3 查询所有粉丝 select * from tb_follow where follow_user_id = ?
        List<Follow> follows = followService.query().eq("follow_user_id", user.getId()).list();
        // 4 推送笔记给所有粉丝的收件箱，此处使用推模式,此处收件箱使用sortSet实现
        for (Follow follow: follows) {
            // 4 1 获取粉丝id
            Long followId = follow.getUserId();
            // 4 2 推送
            String key = RedisConstants.FEED_KEY + followId;
            stringRedisTemplate.opsForZSet().add(key, blog.getId().toString(), System.currentTimeMillis());
        }
        // 3 返回id
        return Result.ok(blog.getId());
    }

    @Override
    public Result queryBlogOfFollow(Long max, Integer offset) {
        // 1 一个用户一个收件箱，获取当前用户
        Long userId = UserHolder.getUser().getId();
        // 2 查询收件箱
        String key = RedisConstants.FEED_KEY + userId;
        // 查询做的是一个分页 ZRANGEBYSCORE key min max LIMIT offset count
        Set<ZSetOperations.TypedTuple<String>> typedTuples = stringRedisTemplate.opsForZSet()
                .reverseRangeByScoreWithScores(key, 0, max, offset, 2);
        if (null == typedTuples || typedTuples.isEmpty()) {
            return Result.ok();
        }

        // 3 解析数据；blogId（但是前段要的是blog的集合）, score（minTime时间戳）,offset(这一次查询到的里面，跟最小值一样的元素个数)
        List<Long> ids = new ArrayList<>(typedTuples.size());
        long minTime = 0;
        int offsetSame = 1;
        for (ZSetOperations.TypedTuple<String> tuple: typedTuples) {
            // 3 1 获取id
            String idStr = tuple.getValue();
            ids.add(Long.valueOf(idStr));

            // 获取分数（时间戳）
            long time = tuple.getScore().longValue();
            if (time == minTime) {
                offsetSame += 1; // 查询这一次查询中与最小值一样的个数
            } else {
                minTime = time;
                offsetSame = 1;  // 自己算一个
            }
        }

        // 4 根据id查询blog，因为要跟查询出来的顺序一样
        String idStr = StrUtil.join(",", ids);
        List<Blog> blogs = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();

        for (Blog blog : blogs) {
             // 查询blog有关的用户
            queryBlogUser(blog);
            // 查询blog是否被点赞
            isBlogLiked(blog);
        }

        // 5 封装并返回
        ScrollResult scrollResult = new ScrollResult();
        scrollResult.setList(blogs);
        scrollResult.setMinTime(minTime);
        scrollResult.setOffset(offsetSame);

        return Result.ok(scrollResult);
    }
}


package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSON;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisData;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    // 封装成一个缓存工具类了
    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
//        // 向缓存写入null解决缓存穿透
//        Shop shop = queryWithPassThroughById(id);
//        Shop shop = cacheClient
//                .queryWithPassThrough(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, id2 -> getById(id2), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

//        // 互斥锁解决缓存击穿
//        Shop shop = queryWithMutexById(id);

        // 注意逻辑过期，由于是热点key，缓存中一定要提前写入热点key，此处用的单元测试
//        // 逻辑过期解决缓存击穿
//        Shop shop = queryWithLogicalExpireById(id);
        Shop shop = cacheClient.queryWithLogicalExpireById(RedisConstants.CACHE_SHOP_KEY, id, Shop.class, this::getById, RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);

        if (null == shop) {
            return Result.ok("店铺不存在！");
        }

        // 返回店铺信息
        return Result.ok(shop);
    }

    // 为解决缓存击穿，使用互斥锁
    public Shop queryWithMutexById(Long id) {
        // 1 从redis查询商户缓存
        // 是个对象，最好用hash，但是此处为了演示，使用string
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);

        // 2 查询是否存在，是否命中
        if (StrUtil.isNotBlank(shopJson)) { // 空字符串，null，换行等都是false
            // 3 存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
//            return Result.ok(shop);
            return null;
        }

        // 解决缓存穿透，将空值（""而不是null）写入，所以判断命中的是否是空值，即空字符串
        if (null != shopJson) { // 此处逻辑是前面isNotBlank判断
            // 返回错误信息
//            return Result.fail("店铺不存在!");
            return null;
        }

        // 4 实现缓存重建
        // 4 1 获取互斥锁
        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            // 4 2 判断是否获取成功
            // 4 3 失败，则休眠并重试
            if (!isLock) {
                Thread.sleep(50);
                return queryWithMutexById(id); // 递归，睡眠后，再去查询
            }
            // 4 4 成功，根据ID查询数据库
            shop = getById(id);
            // 因为缓存击穿，是高并发和重建慢，所以此处模拟重建的延时
            Thread.sleep(200);

            // 5 数据库也不存在，返回错误
            if (null == shop) {
                // 为了解决穿透缓存，需要将空值写入
                stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
    //            return Result.fail("店铺不存在!");
                return null;
            }
            // 6 存在，写入redis
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 7 释放互斥锁
            unLock(lockKey);
        }


        // 返回店铺信息
//        return Result.ok(shop);
        return shop;
    }

//    // 为解决缓存击穿，使用逻辑过期
//    // 热点key一般会提前放入redis
//    public Shop queryWithLogicalExpireById(Long id) {
//        // 1 从redis查询商户缓存
//        // 是个对象，最好用hash，但是此处为了演示，使用string
//        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
//
//        // 2 查询是否存在，是否命中
//        if (StrUtil.isBlank(shopJson)) {
//            // 3 不存在，直接返回，因为一般热点代码，都会提前放入redis中的
//            return null;
//        }
//
//        // 4 命中，需要判断过期时间，把json反序列化为对象
//        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
//        JSONObject data = (JSONObject) redisData.getData();
//        Shop shop = JSONUtil.toBean(data, Shop.class);
//        LocalDateTime expireTime = redisData.getExpireTime();
//
//        // 5 判断是否过期
//        if (expireTime.isAfter(LocalDateTime.now())) {
//            // 5 1 未过期，直接返回店铺信息
//            return shop;
//        }
//
//        // 5 2 已经过期，需要缓存重建
//        // 6 缓存重建
//        // 6 1 获取互斥锁
//        String lockKey = RedisConstants.LOCK_SHOP_KEY + id;
//        boolean isLock = tryLock(lockKey);
//
//        // 6 2 判断是否获取锁成功
//        if (isLock) {
//            // 6 3 成功，开启独立线程，执行重建
//            CACHE_REBUILD_EXECUTOR.submit(() -> {
//                try {
//                    // 重建缓存
//                    this.saveShop2Redis(id, 30L);
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                } finally {
//                    // 释放锁
//                    unLock(lockKey);
//                }
//            });
//        }
//
//        // 6 4 返回过期的商铺信息
//        return shop;
//    }
//
//    // 线程池
//    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);
//
//    // 为解决缓存穿透，使用向缓存写入null方案
//    public Shop queryWithPassThroughById(Long id) {
//        // 1 从redis查询商户缓存
//        // 是个对象，最好用hash，但是此处为了演示，使用string
//        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY + id);
//
//        // 2 查询是否存在，是否命中
//        if (StrUtil.isNotBlank(shopJson)) { // 空字符串，null，换行等都是false
//            // 3 存在，直接返回
//            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
////            return Result.ok(shop);
//            return null;
//        }
//
//        // 解决缓存穿透，将空值（""而不是null）写入，所以判断命中的是否是空值，即空字符串
//        if (null != shopJson) { // 此处逻辑是前面isNotBlank判断
//            // 返回错误信息
////            return Result.fail("店铺不存在!");
//            return null;
//        }
//
//
//        // 4 不存在，根据ID查询数据库
//        Shop shop = getById(id);
//
//        // 5 数据库也不存在，返回错误
//        if (null == shop) {
//            // 为了解决穿透缓存，需要将空值写入
//            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, "", RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
////            return Result.fail("店铺不存在!");
//            return null;
//        }
//        // 6 存在，写入redis
//        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(shop), RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
//
//        // 返回店铺信息
////        return Result.ok(shop);
//        return shop;
//    }


    // 把热点数据缓存写入redis，里面又封装了一个data（包含shop和expiretime），永久有效的，需要逻辑更新
    // 写了单元测试来实现，还有缓存重建
    public void saveShop2Redis(Long id, Long expireSeconds) throws InterruptedException {
        // 1 查询店铺数据
        Shop shop = getById(id);
        // 模拟重建延迟
//        Thread.sleep(200);

        // 2 封装逻辑过期时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));

        // 写入Redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY + id, JSONUtil.toJsonStr(redisData));
    }

    // 由于需要保证redis和mysql的一致性
    // 调试的时候，使用postman来put，访问的是http://localhost:8081/shop
    @Override
    @Transactional    // 整个方法是一个统一事物，单体项目没出现异常可以回滚
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (null == id) {
            return Result.fail("店铺不能为空");
        }

        System.out.println(shop);

        // 1 先更新数据库
        updateById(shop);
        // 2 再删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY + id);

        return Result.ok();
    }

    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1 判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 根据类型分页查询
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        // 2 计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3 查询redis，按照距离排序，分页，结果，shopId，distance
        // GEOSEARCH BYLONGLAT x y BYRADIUS 10 WITHDISTANCE
        String key = RedisConstants.SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo()
                .search(key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000), // 5000m
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 注意，上面那个limit只能从0开始拿，所以此处需要截取from到end的店铺

        // 4 解析出id
        if (null == results) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 没有下一页，结束
            return Result.ok(Collections.emptyList());
        }
        // 4 1 截取from～end部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4 2 获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4 3 获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });

        // 5 根据id查询shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6 返回
        return Result.ok(shops);
    }


    // 通过不存在才写入，不存在写入成功为1，存在不成功为0
    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);

        // flag返回会做拆箱的，拆箱可能为空，如果flag为空的话，拆箱可能为空
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key) {
        stringRedisTemplate.delete(key);
    }
}

package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.SeckillVoucher;
import com.hmdp.entity.Voucher;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.SimpleRedisLock;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {
    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 * 1024);
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor(); // 线程池，处理订单

    // 处理订单的阻塞队列在程序运行就可以有，所以这个注解表示这个方法在类初始化之后就运行，然后他调用了线程运行异步队列
    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
//    // 使用java自己的阻塞队列BlockingQueue的代码
//    private class VoucherOrderHandler implements Runnable {
//        @Override
//        public void run() {
//            while(true) {
//                // 1 获取队列中的订单
//                try {
//                    VoucherOrder voucherOrder = orderTasks.take(); // 尽管写的是whiletrue，但是此处的take在没有的时候会阻塞掉
//                    // 2 创建订单
//                    createVoucherOrder(voucherOrder);
//                } catch (InterruptedException e) {
//                    throw new RuntimeException(e);
//                }
//            }
//        }
//    }

    // 获取redis的消息队列STREAMS的代码
    private class VoucherOrderHandler implements Runnable {
        @Override
        public void run() {
            String queueName = "stream.orders";
            while(true) {

                try {
                    // 1 获取队列中的订单  XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );

                    // 2 判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明没有消息，继续下一次循环
                        continue;
                    }

                    // 3 创建订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);

                    // 4 确认消息XACK
                    createVoucherOrder(voucherOrder);
                    // 2 创建订单
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常！");
                    handlePenddingList();
//                    throw new RuntimeException(e);
                }
            }
        }

        // 处理过程中有异常，需要从pendinglist中得到没有ack的消息
        private void handlePenddingList() {
            String queueName = "stream.orders";
            while(true) {
                try {
                    // 1 获取pendinglist中的订单，从0开始  XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS s1 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );

                    // 2 判断订单信息是否为空
                    if (list == null || list.isEmpty()) {
                        // 如果为null，说明pendinglist中没有异常消息，结束循环
                        break;
                    }

                    // 3 创建订单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);

                    // 4 确认消息XACK
                    createVoucherOrder(voucherOrder);
                    // 2 创建订单
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理pendinglist订单异常！");
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                }
            }
        }
    }

    private void createVoucherOrder(VoucherOrder voucherOrder) {
        // 注意，此处的很多判断，其实肯定是无效的，因为前面lua判断过了，这里只不过是再稍微判断一下而已
        long voucherId  = voucherOrder.getVoucherId();
        Long userId = voucherOrder.getUserId();
        // 这个一人一单，是插入数据的，不能用乐观锁，所以此处用悲观锁，防止多个线程是一个用户的时候同时访问而出错
        // 5 一人一单
        // 5 1 查询订单

        // 创建锁对象
        RLock redisLock = redissonClient.getLock("order:" + userId);
        // 尝试获取锁
//        boolean isLock = redisLock.tryLock(1, 10, TimeUnit.SECONDS);
        boolean isLock = redisLock.tryLock(); // 里面不写参数的话，失败了直接结束，不会像上面1s内重试。一次，不重试
        // 判断
        if (!isLock) { // 失败，可以重试，也可以直接返回失败
//            return Result.fail("不允许重复下单！");
            log.error("不允许重复下单！");
            return;
        }

        try {
            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
            // 5 2 判断是否存在（通过用户id和优惠券id）
            if (count > 0) {
//                return Result.fail("用户已经购买过一次!");
                log.error("用户已经购买过一次!");
                return;
            }

            // 6 扣减库存，数据库里面的
//        boolean success = seckillVoucherService.update()
//                .setSql("stock = stock - 1")    // set stock = stock - 1
//                .eq("voucher_id", voucherId).eq("stock", voucher.getStock()) // 乐观锁，CAS， voucher.getStock()是旧数值，数据库拿到的是新数值，where id = ? and stock = ?
//                .update();
            boolean success = seckillVoucherService.update()
                    .setSql("stock = stock - 1")    // set stock = stock - 1
                    .eq("voucher_id", voucherId).gt("stock", 0) // 此处优化，没必要每一次都相等才给数值，此处直接>0就好了
                    .update();

            if (!success) {
//                return Result.fail("库存不足");
                log.error("库存不足");
            }

            // 7 保存订单到数据库
            save(voucherOrder);  // 存储到数据库

            // 8 返回订单id
//            return Result.ok(order);
        } finally {
            redisLock.unlock();
        }
    }

    // 静态加一次，而不是说每次判断释放锁的时候，每次加载一次
    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    // 使用redis的阻塞队列STREAM的写法，lua中获得资格后，直接想队列中写入了订单的信息
    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        Long orderId = redisIdWorker.nextId("order");

        // 1 执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), orderId.toString()
        );
        int r = result.intValue();
        // 2 判断是否为0
        if (r != 0) {
            // 2 1 不为0，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }

        // 2 2 为0，有购买资格，把下单信息保存到阻塞队列


        // 3 返回订单id
        return Result.ok(orderId);
    }

//    // 使用java自己的阻塞队列BlockingQueue的写法
//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        Long userId = UserHolder.getUser().getId();
//
//        // 1 执行lua脚本
//        Long result = stringRedisTemplate.execute(
//                SECKILL_SCRIPT,
//                Collections.emptyList(),
//                voucherId.toString(), userId.toString()
//        );
//        int r = result.intValue();
//        // 2 判断是否为0
//        if (r != 0) {
//            // 2 1 不为0，代表没有购买资格
//            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
//        }
//
//        // 2 2 为0，有购买资格，把下单信息保存到阻塞队列
//        long orderId = redisIdWorker.nextId("order");
//
//        // TODO 保存阻塞队列s
//        VoucherOrder voucherOrder = new VoucherOrder();
//        voucherOrder.setId(orderId);
//        voucherOrder.setUserId(userId);
//        voucherOrder.setVoucherId(voucherId);
//        // 放入阻塞队列
//        orderTasks.add(voucherOrder);
//
//        // 3 返回订单id
//        return Result.ok(orderId);
//    }


//    @Override
//    public Result seckillVoucher(Long voucherId) {
//        // 1 查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//
//        // 2 判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            return Result.fail("秒杀尚未开始");
//        }
//
//        // 3 判断是否已经结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            return Result.fail("秒杀已经结束");
//        }
//
//        // 4 判断库存是否充足
//        if (voucher.getStock() < 1) {
//            return Result.fail("库存不足");
//        }
//
//        return createVoucherOrder(voucherId);
//    }



//    // 此处用的第三方的分布式锁，可以保证集群模式时候的一人一单
//    @Transactional // 订单新增，库存减，两张表的操作，最好加事务
//    public Result createVoucherOrder(Long voucherId) {
//        // 这个一人一单，是插入数据的，不能用乐观锁，所以此处用悲观锁，防止多个线程是一个用户的时候同时访问而出错
//        // 5 一人一单
//        // 5 1 查询订单
//        Long userId = UserHolder.getUser().getId(); // UserHolder中拿到的
//
//        // 创建锁对象
//        RLock redisLock = redissonClient.getLock("order:" + userId);
//        // 尝试获取锁
////        boolean isLock = redisLock.tryLock(1, 10, TimeUnit.SECONDS);
//        boolean isLock = redisLock.tryLock(); // 里面不写参数的话，失败了直接结束，不会像上面1s内重试。一次，不重试
//        // 判断
//        if (!isLock) { // 失败，可以重试，也可以直接返回失败
//            return Result.fail("不允许重复下单！");
//        }
//
//        try {
//            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//            // 5 2 判断是否存在（通过用户id和优惠券id）
//            if (count > 0) {
//                return Result.fail("用户已经购买过一次!");
//            }
//
//            // 6 扣减库存
////        boolean success = seckillVoucherService.update()
////                .setSql("stock = stock - 1")    // set stock = stock - 1
////                .eq("voucher_id", voucherId).eq("stock", voucher.getStock()) // 乐观锁，CAS， voucher.getStock()是旧数值，数据库拿到的是新数值，where id = ? and stock = ?
////                .update();
//            boolean success = seckillVoucherService.update()
//                    .setSql("stock = stock - 1")    // set stock = stock - 1
//                    .eq("voucher_id", voucherId).gt("stock", 0) // 此处优化，没必要每一次都相等才给数值，此处直接>0就好了
//                    .update();
//
//            if (!success) {
//                return Result.fail("库存不足");
//            }
//
//            // 7 创建订单
//            VoucherOrder voucherOrder = new VoucherOrder();
//            // 订单id
//            long order = redisIdWorker.nextId("order");
//            voucherOrder.setId(order);
//            // 用户id
//            voucherOrder.setUserId(userId);
//            // 代金券id
//            voucherOrder.setVoucherId(voucherId);
//            save(voucherOrder);  // 存储到数据库
//
//            // 8 返回订单id
//            return Result.ok(order);
//        } finally {
//            redisLock.unlock();
//        }
//    }

//    // 此处用的分布式锁，可以保证集群模式时候的一人一单
//    @Transactional // 订单新增，库存减，两张表的操作，最好加事务
//    public Result createVoucherOrder(Long voucherId) {
//        // 这个一人一单，是插入数据的，不能用乐观锁，所以此处用悲观锁，防止多个线程是一个用户的时候同时访问而出错
//        // 5 一人一单
//        // 5 1 查询订单
//        Long userId = UserHolder.getUser().getId(); // UserHolder中拿到的
//
//        // 创建锁对象
//        SimpleRedisLock simpleRedisLocks = new SimpleRedisLock("order:" + userId, stringRedisTemplate);// 注意，只要同一个用的的，锁一下就好了
//        // 尝试获取锁
//        boolean isLock = simpleRedisLocks.tryLock(1200L);
//        // 判断
//        if (!isLock) { // 失败，可以重试，也可以直接返回失败
//            return Result.fail("不允许重复下单！");
//        }
//
//        try {
//            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//            // 5 2 判断是否存在（通过用户id和优惠券id）
//            if (count > 0) {
//                return Result.fail("用户已经购买过一次!");
//            }
//
//            // 6 扣减库存
////        boolean success = seckillVoucherService.update()
////                .setSql("stock = stock - 1")    // set stock = stock - 1
////                .eq("voucher_id", voucherId).eq("stock", voucher.getStock()) // 乐观锁，CAS， voucher.getStock()是旧数值，数据库拿到的是新数值，where id = ? and stock = ?
////                .update();
//            boolean success = seckillVoucherService.update()
//                    .setSql("stock = stock - 1")    // set stock = stock - 1
//                    .eq("voucher_id", voucherId).gt("stock", 0) // 此处优化，没必要每一次都相等才给数值，此处直接>0就好了
//                    .update();
//
//            if (!success) {
//                return Result.fail("库存不足");
//            }
//
//            // 7 创建订单
//            VoucherOrder voucherOrder = new VoucherOrder();
//            // 订单id
//            long order = redisIdWorker.nextId("order");
//            voucherOrder.setId(order);
//            // 用户id
//            voucherOrder.setUserId(userId);
//            // 代金券id
//            voucherOrder.setVoucherId(voucherId);
//            save(voucherOrder);  // 存储到数据库
//
//            // 8 返回订单id
//            return Result.ok(order);
//        } finally {
//            simpleRedisLocks.unlock();
//        }
//    }

//    // 此处用的synchronized，不能保证集群模式时候的一人一单
//    @Transactional // 订单新增，库存减，两张表的操作，最好加事务
//    public Result createVoucherOrder(Long voucherId) {
//        // 这个一人一单，是插入数据的，不能用乐观锁，所以此处用悲观锁，防止多个线程是一个用户的时候同时访问而出错
//        // 5 一人一单
//        // 5 1 查询订单
//        Long userId = UserHolder.getUser().getId(); // UserHolder中拿到的
//
//        // 注意：这里面userId.toString().intern()需要注意，id一样的是同一个用户，期望id一样的每次只能访问一个
//        // 但是toString底层是new了一个String，所以此处用inter，这里去字符串常量池找一个有没有值一样的引用返回，这样才对
//        // synchronized里面的关键字比较的是对象的引用，toString底层newString的话对象也是不一样的
//        synchronized (userId.toString().intern()) {
//            int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
//            // 5 2 判断是否存在（通过用户id和优惠券id）
//            if (count > 0) {
//                return Result.fail("用户已经购买过一次!");
//            }
//
//            // 6 扣减库存
////        boolean success = seckillVoucherService.update()
////                .setSql("stock = stock - 1")    // set stock = stock - 1
////                .eq("voucher_id", voucherId).eq("stock", voucher.getStock()) // 乐观锁，CAS， voucher.getStock()是旧数值，数据库拿到的是新数值，where id = ? and stock = ?
////                .update();
//            boolean success = seckillVoucherService.update()
//                    .setSql("stock = stock - 1")    // set stock = stock - 1
//                    .eq("voucher_id", voucherId).gt("stock", 0) // 此处优化，没必要每一次都相等才给数值，此处直接>0就好了
//                    .update();
//
//            if (!success) {
//                return Result.fail("库存不足");
//            }
//
//            // 7 创建订单
//            VoucherOrder voucherOrder = new VoucherOrder();
//            // 订单id
//            long order = redisIdWorker.nextId("order");
//            voucherOrder.setId(order);
//            // 用户id
//            voucherOrder.setUserId(userId);
//            // 代金券id
//            voucherOrder.setVoucherId(voucherId);
//            save(voucherOrder);  // 存储到数据库
//
//            // 8 返回订单id
//            return Result.ok(order);
//        }
//    }
}

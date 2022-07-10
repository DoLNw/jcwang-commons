-- 使用lua脚本可以保证判断和释放锁是由原子性的
-- 数组从1开始
-- 比较线程标识与锁中的是否一致
if (redis.call('get', KEYS[1]) == ARGV[1]) then
    -- 释放锁
    return redis.call('del', KEYS[1])
end
-- 不一致，返回0
return 0
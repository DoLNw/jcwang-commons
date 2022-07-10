package com.hmdp.utils;

public interface ILock {
    /**
     * 尝试获取锁
     * timeoutSec锁持有的超市时间，过期后自动释放
     * return true代表获取锁成功，
     */
    boolean tryLock(Long timeoutSec);

    /**
     * 释放锁
     */
    void unlock();
}

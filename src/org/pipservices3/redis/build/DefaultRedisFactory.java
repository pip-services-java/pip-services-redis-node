package org.pipservices3.redis.build;

import org.pipservices3.commons.refer.Descriptor;
import org.pipservices3.components.build.Factory;
import org.pipservices3.redis.cache.RedisCache;
import org.pipservices3.redis.lock.RedisLock;

/**
 * Creates Redis components by their descriptors.
 *
 * @see RedisCache
 * @see RedisLock
 */
public class DefaultRedisFactory extends Factory {
    private static final Descriptor RedisCacheDescriptor = new Descriptor("pip-services", "cache", "redis", "*", "1.0");
    private static final Descriptor RedisLockDescriptor = new Descriptor("pip-services", "lock", "redis", "*", "1.0");

    /**
     * Create a new instance of the factory.
     */
    public DefaultRedisFactory() {
        super();
        this.registerAsType(DefaultRedisFactory.RedisCacheDescriptor, RedisCache.class);
        this.registerAsType(DefaultRedisFactory.RedisLockDescriptor, RedisLock.class);
    }
}

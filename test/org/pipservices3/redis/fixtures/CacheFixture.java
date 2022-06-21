package org.pipservices3.redis.fixtures;

import org.pipservices3.commons.convert.JsonConverter;
import org.pipservices3.components.cache.ICache;
import org.pipservices3.redis.cache.RedisCache;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class CacheFixture {
    private ICache _cache;

    String KEY1 = "key1";
    String KEY2 = "key2";
    String KEY3 = "key3";
    String KEY4 = "key4";
    String KEY5 = "key5";
    String KEY6 = "key6";

    String VALUE1 = "value1";
    Map<String, String> VALUE2 = Map.of("val", "value2");
    ZonedDateTime VALUE3 = ZonedDateTime.now();
    List<Integer> VALUE4 = List.of(1, 2, 3, 4);
    int VALUE5 = 12345;
    Object VALUE6 = null;

    public CacheFixture(ICache cache) {
        this._cache = cache;
    }

    public void testStoreAndRetrieve() throws InterruptedException, IOException {
        this._cache.store(null, KEY1, VALUE1, 5000);
        this._cache.store(null, KEY2, VALUE2, 5000);
        this._cache.store(null, KEY3, VALUE3, 5000);
        this._cache.store(null, KEY4, VALUE4, 5000);
        this._cache.store(null, KEY5, VALUE5, 5000);
        this._cache.store(null, KEY6, VALUE6, 5000);

        Thread.sleep(500);

        var val = this._cache.retrieve(null, KEY1);

        assertNotNull(val);
        assertEquals(VALUE1, val);

        val = this._cache.retrieve(null, KEY2);
        var resMap = JsonConverter.toMap(val.toString());
        assertNotNull(resMap);
        assertEquals(VALUE2.get("val"), resMap.get("val"));

        val = this._cache.retrieve(null, KEY3);
        assertNotNull(val);
        assertEquals(VALUE3.withZoneSameInstant(ZoneId.of("UTC"))
                .format(DateTimeFormatter.ISO_OFFSET_DATE_TIME), val.toString());

        val = this._cache.retrieve(null, KEY4);
        var resList = JsonConverter.fromJson(List.class, (String) val);
        assertNotNull(resList);
        assertEquals(resList.size(), 4);
        assertEquals(VALUE4.get(0), resList.get(0));

        val = this._cache.retrieve(null, KEY5);
        var resInt = Integer.parseInt((String)val);
        assertNotNull(resInt);
        assertEquals(VALUE5, resInt);

        val = this._cache.retrieve(null, KEY6);
        assertNotNull(val);
    }

    public void testRetrieveExpired() throws InterruptedException {
        this._cache.store(null, KEY1, VALUE1, 1000);

        Thread.sleep(1500);

        var val = this._cache.retrieve(null, KEY1);
        assertNull(val);
    }

    public void testRemove() {
        this._cache.store(null, KEY1, VALUE1, 1000);

        this._cache.remove(null, KEY1);

        var val = this._cache.retrieve(null, KEY1);
        assertNull(val);
    }
}

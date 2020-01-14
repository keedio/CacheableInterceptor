package org.keedio.flume.interceptor.cacheable.service;

//import org.keedio.flume.interceptor.cacheable.service.ICacheService;
import com.google.common.collect.ImmutableMap;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import org.apache.flume.Event;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:beans.xml"})
public class FlumeCacheServiceTest {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(FlumeCacheServiceTest.class);

    @Autowired
    private ICacheService<Event> service;

    @Autowired
    private CacheManager cacheManager;

    private Cache cache;

    @PostConstruct
    public void init() {
        service.setProperties(ImmutableMap.of(
                ICacheService.PROPERTIES_CSV_SEPARATOR, ",",
                ICacheService.PROPERTIES_CSV_DIRECTORY, "src/main/resources"));

        cache = cacheManager.getCache("FlumeCachedEvent");
    }

    /**
     * Test of enrichMap method, of class FlumeCacheService.
     */
    @Test
    public void testEnrichMapSingleKey() throws IOException {

        System.out.println("enrichMapSingleKey");
        Map<String, String> data = new HashMap<String, String>();
        Map<String, String> data2 = new HashMap<String, String>();
        data.put("ciid", "124");
        data.put("vdc", "01");
        data.put("item", "proxy");
        data.put("delivery", "***REMOVED***");

        data2.put("hostname", "host1");

        assertEquals(data, service.enrichMap(data2));
    }


    @Test
    public void testEnrichMapMultipleKeys() throws IOException {

        System.out.println("enrichMapMultipleKeys");
        Map<String, String> data = new HashMap<String, String>();
        Map<String, String> data2 = new HashMap<String, String>();
        data.put("ciid", "124");
        data.put("vdc", "01");
        data.put("delivery", "***REMOVED***");

        data2.put("hostname", "host1");
        data2.put("item", "proxy");

        Map<String, String> mapResult = service.enrichMap(data2);

        assertEquals(data, mapResult);
    }

    @Test
    public void testEnrichMapMultipleKeysNoMatch() throws IOException {

        System.out.println("enrichMapEnrichMapMultipleKeysNoMatch");
        Map<String, String> data2 = new HashMap<String, String>();

        data2.put("hostname", "host1");
        data2.put("item", "FW");

        Map<String, String> mapResult = service.enrichMap(data2);
        System.out.println(mapResult.toString());
        org.junit.Assert.assertTrue(mapResult.isEmpty());
    }

    @Test
    public void testCacheEviction() throws IOException {

        int cacheInitialSize = cache.getSize();

        Map<String, String> data = new HashMap<>();
        data.put("hostname", "host1");

        service.enrichMap(data);
        assertEquals(cacheInitialSize + 1, cache.getSize());

        service.evictCache();
        assertEquals(0, cache.getSize());
    }

}

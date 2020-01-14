package org.keedio.flume.interceptor.cacheable.service;

import org.keedio.flume.interceptor.cacheable.service.ICacheService;
import com.google.common.collect.ImmutableMap;
import org.apache.flume.Event;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.junit.Ignore;

import static org.junit.Assert.assertEquals;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:beans.xml"})
public class FlumeCacheServiceWithFilterTest {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(FlumeCacheServiceTest.class);

    @Autowired
    private ICacheService<Event> service2;

    @PostConstruct
    public void init() {
        Map<String, String> criterias = new HashMap<>();
        criterias.put("1", "{\"key\":{\"hostname\":\"host1\",\"ciid\":\"124\"},\"values\":[\"item\",\"delivery\"]}");
        
        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        Map<String, String> params = builder
                .putAll(criterias)
                .put(ICacheService.PROPERTIES_CSV_SEPARATOR, ",")
                .put(ICacheService.PROPERTIES_CSV_DIRECTORY, "src/main/resources")
                .build();

        service2.setProperties(params);
    }

    /**
     * Test of enrichMap method, of class FlumeCacheService.
     */
    @Test
    public void testEnrichMapSingleKeyFilter() throws IOException {

        System.out.println("enrichMapSingleKeyAndFilter");
        Map<String, String> data = new HashMap<String, String>();
        Map<String, String> data2 = new HashMap<String, String>();
        
        data.put("delivery", "***REMOVED***");
        data.put("item", "proxy");

        data2.put("hostname", "host1");
        
        assertEquals(data, service2.enrichMap(data2));
    }

    
    @Test
    public void testEnrichMapMultipleKeysFilter() throws IOException {

        System.out.println("enrichMapMultipleKeysAndFilter");
        Map<String, String> data = new HashMap<String, String>();
        Map<String, String> data2 = new HashMap<String, String>();

        
        data.put("item", "proxy");
        data.put("delivery", "***REMOVED***");

        data2.put("hostname", "host1");
        data2.put("item", "proxy");

        Map<String, String> mapResult = service2.enrichMap(data2);

        assertEquals(data, mapResult);
    }

    @Test
    public void testEnrichMapMultipleKeysNoMatchFilter() throws IOException {

        System.out.println("enrichMapEnrichMapMultipleKeysNoMatchAndFilter");
        Map<String, String> data2 = new HashMap<String, String>();

        data2.put("hostname", "host1");
        data2.put("item", "FW");

        Map<String, String> mapResult = service2.enrichMap(data2);
        System.out.println(mapResult.toString());
        org.junit.Assert.assertTrue(mapResult.isEmpty());
    }

}

package org.keedio.flume.interceptor.cacheable;

import org.keedio.flume.interceptor.cacheable.interceptor.CacheableInterceptor;
import com.google.common.collect.ImmutableMap;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.keedio.flume.interceptor.cacheable.service.CacheEventListener;
import org.keedio.flume.interceptor.cacheable.service.ICacheService;
import org.keedio.flume.interceptor.cacheable.service.LockfileWatchdogTest;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.annotation.PostConstruct;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBody;
import org.keedio.flume.interceptor.cacheable.service.ICacheService;

import static org.keedio.flume.interceptor.cacheable.service.ICacheService.*;
import static org.keedio.flume.interceptor.cacheable.serialization.JSONStringSerializer.fromJSONString;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:beans.xml")
public class CacheableInterceptorTest {
    private Logger logger = Logger.getLogger(getClass());

    @Autowired
    private ApplicationContext springContext;

    private CacheableInterceptor interceptor;

    @Autowired
    private CacheEventListener listener;

    @Autowired
    private CacheManager cacheManager;

    private Cache cache;

    private Context flumeContext;
    private static final String INTERVAL = "10";

    @PostConstruct
    public void init() {

        flumeContext = mock(Context.class);

        when(flumeContext.getString(PROPERTIES_CSV_SEPARATOR)).thenReturn(",");
        when(flumeContext.getString(PROPERTIES_CSV_DIRECTORY)).thenReturn("src/main/resources");
        when(flumeContext.getString(PROPERTIES_LOCKFILE)).thenReturn(LockfileWatchdogTest.LOCKFILE);
        when(flumeContext.getString(PROPERTIES_WATCH_INTERVAL)).thenReturn(INTERVAL);
        CacheableInterceptor.Builder builder = new CacheableInterceptor.Builder();
        builder.configure(flumeContext);
        builder.setSpringContext(springContext);

        interceptor = (CacheableInterceptor) builder.build();
        interceptor.initialize();

        cache = cacheManager.getCache("FlumeCachedEvent");
    }

    @Before
    public void initMethod() {
        cache.clear();
        listener.resetCounter();
    }


    @Test
    public void testSpringService() throws IOException {

        ICacheService<Event> service = interceptor.getService();
        assertNotNull(service);

        String name = service.getClass().getCanonicalName();
        assertNotNull(name);
    }

    @Test
    public void testEnrich() throws IOException {

        String testString = "hola";

        Map<String, String> extraData = ImmutableMap.of(
                "hostname", "host1",
                "item", "proxy");

        EnrichedEventBody eeb = new EnrichedEventBody(extraData, testString);

        List<Event> events = new ArrayList<>();

        int numEvents = 200;
        for (int i = 0; i < numEvents; i++) {
            SimpleEvent se = new SimpleEvent();
            se.setBody(eeb.buildEventBody());
            events.add(se);
        }

        List<Event> result = interceptor.intercept(events);

        Map<String, String> resData = ImmutableMap.of(
                "hostname", "host1",
                "item", "proxy",
                "ciid", "124",
                "vdc", "01",
                "delivery", "***REMOVED***");

        for (Event e : result) {
            EnrichedEventBody resEEB =
                    fromJSONString(new String(e.getBody()), EnrichedEventBody.class);

            assertNotNull(resEEB);
            assertEquals(resData, resEEB.getExtraData());
        }

        assertEquals(1, listener.getCounterValue());
    }

    @Test
    public void testEnrichTwoSteps() throws IOException {
        String testString = "hola";

        Map<String, String> extraData0 = ImmutableMap.of(
                "hostname", "host1",
                "item", "proxy");

        Map<String, String> extraData1 = new HashMap<>();
        extraData1.put("hostname", "host2");
        extraData1.put("item", "proxy");

        Map<String, String> extraData2 = new HashMap<>();
        extraData2.put("hostname", "host2");
        extraData2.put("item", "proxy");

        Map<String, String> extraData3 = new HashMap<>();
        extraData3.put("hostname", "host2");
        extraData3.put("item", "FW");

        EnrichedEventBody eeb0 = new EnrichedEventBody(extraData0, testString);
        EnrichedEventBody eeb1 = new EnrichedEventBody(extraData1, testString);
        EnrichedEventBody eeb2 = new EnrichedEventBody(extraData2, testString);
        EnrichedEventBody eeb3 = new EnrichedEventBody(extraData3, testString);

        List<Event> events = new ArrayList<>();

        SimpleEvent se0 = new SimpleEvent();
        se0.setBody(eeb0.buildEventBody());
        events.add(se0);

        SimpleEvent se1 = new SimpleEvent();
        se1.setBody(eeb1.buildEventBody());
        events.add(se1);

        SimpleEvent se2 = new SimpleEvent();
        se2.setBody(eeb2.buildEventBody());
        events.add(se2);

        SimpleEvent se3 = new SimpleEvent();
        se3.setBody(eeb3.buildEventBody());
        events.add(se3);

        Event result = interceptor.intercept(events.get(0));

        Map<String, String> resData0 = ImmutableMap.of(
                "hostname", "host1",
                "item", "proxy",
                "ciid", "124",
                "vdc", "01",
                "delivery", "***REMOVED***");

        Map<String, String> resData3 = ImmutableMap.of(
                "hostname", "host2",
                "item", "FW",
                "ciid", "211",
                "vdc", "02",
                "delivery", "Cantabria");

        EnrichedEventBody resEEB0 =
                fromJSONString(new String(result.getBody()), EnrichedEventBody.class);

        assertNotNull(resEEB0);
        assertEquals(resData0, resEEB0.getExtraData());

        result = interceptor.intercept(events.get(1));

        EnrichedEventBody resEEB1 =
                fromJSONString(new String(result.getBody()), EnrichedEventBody.class);

        assertNotNull(resEEB1);
        assertEquals(extraData1, resEEB1.getExtraData());

        result = interceptor.intercept(events.get(2));

        EnrichedEventBody resEEB2 =
                fromJSONString(new String(result.getBody()), EnrichedEventBody.class);

        assertNotNull(resEEB2);
        assertEquals(extraData2, resEEB2.getExtraData());

        result = interceptor.intercept(events.get(3));

        EnrichedEventBody resEEB3 =
                fromJSONString(new String(result.getBody()), EnrichedEventBody.class);

        assertNotNull(resEEB3);
        assertEquals(resData3, resEEB3.getExtraData());

        assertEquals(3, listener.getCounterValue());
    }

    @Test
    public void testScheduler() throws IOException, InterruptedException {

        logger.info("Creating temp file: " + flumeContext.getString(PROPERTIES_LOCKFILE));

        File lockfile = new File(flumeContext.getString(PROPERTIES_LOCKFILE));
        long interval = Long.valueOf(flumeContext.getString(PROPERTIES_WATCH_INTERVAL)) * 1000L;

        // Wait less than interval to create the lock file
        Thread.sleep(interval / 3L);
        lockfile.createNewFile();

        assertTrue(lockfile.exists());
        Thread.sleep(interval);
        assertFalse(lockfile.exists());
    }
}

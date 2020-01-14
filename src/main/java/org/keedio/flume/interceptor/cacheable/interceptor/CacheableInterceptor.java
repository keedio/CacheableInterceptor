package org.keedio.flume.interceptor.cacheable.interceptor;

import com.google.common.collect.ImmutableMap;

import java.io.IOException;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.keedio.flume.interceptor.cacheable.service.ICacheService;
import org.keedio.flume.interceptor.cacheable.service.LockfileWatchdog;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.keedio.flume.interceptor.enrichment.interceptor.EnrichedEventBody;
import org.apache.flume.interceptor.Interceptor;

import static org.apache.commons.lang.StringUtils.defaultIfEmpty;
import static org.keedio.flume.interceptor.cacheable.service.ICacheService.*;
import static org.keedio.flume.interceptor.cacheable.serialization.JSONStringSerializer.fromJSONString;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implements a cacheable interceptor
 *
 * @see
 * <a href="http://docs.spring.io/spring/docs/current/spring-framework-reference/html/cache.html">Spring
 * Cache docs</a>
 */
public class CacheableInterceptor implements Interceptor {

    private static final Logger logger = LoggerFactory.getLogger(CacheableInterceptor.class);

    ApplicationContext springContext;
    ICacheService<Event> service;
    private Context flumeContext;
    private ScheduledExecutorService scheduler;
    private Runnable watchdog;

    public CacheableInterceptor(Context flumeContext, ApplicationContext springContext) {
        this.flumeContext = flumeContext;

        // Get Spring springContext
        if (springContext == null) {
            this.springContext = new ClassPathXmlApplicationContext("beans.xml");
        } else {
            this.springContext = springContext;
        }
    }

    @Override
    public void initialize() {
        // Get cache service instance
        service = springContext.getBean(ICacheService.class);
        watchdog = new LockfileWatchdog(flumeContext.getString(PROPERTIES_LOCKFILE), service);

        String separator = defaultIfEmpty(flumeContext.getString(PROPERTIES_CSV_SEPARATOR), DEFAULT_CSV_SEPARATOR);
        String quoteChar = defaultIfEmpty(flumeContext.getString(PROPERTIES_CSV_QUOTE_CHAR), DEFAULT_CSV_QUOTE_CHAR);
        String directory = defaultIfEmpty(flumeContext.getString(PROPERTIES_CSV_DIRECTORY), "");
        String interval = defaultIfEmpty(flumeContext.getString(PROPERTIES_WATCH_INTERVAL), "10");

        logger.debug("Scheduling watchdog...");
        scheduler = Executors.newScheduledThreadPool(1);
        scheduler.scheduleAtFixedRate(watchdog, 0, Long.valueOf(interval), TimeUnit.SECONDS);
        Map<String, String> criterias = flumeContext.getSubProperties(PROPERTIES_CRITERIA_SELECTION);

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

        if (criterias != null) {
            Map<String, String> params = builder
                    .putAll(criterias)
                    .put(PROPERTIES_CSV_SEPARATOR, separator)
                    .put(PROPERTIES_CSV_DIRECTORY, directory)
                    .put(PROPERTIES_CSV_QUOTE_CHAR, quoteChar)
                    .build();

            service.setProperties(params);
        } else {
            Map<String, String> params = builder
                    .put(PROPERTIES_CSV_SEPARATOR, separator)
                    .put(PROPERTIES_CSV_DIRECTORY, directory)
                    .put(PROPERTIES_CSV_QUOTE_CHAR, quoteChar)
                    .build();

            service.setProperties(params);
        }
    }

    @Override
    public Event intercept(Event event) {
        byte[] payload = event.getBody();
        logger.debug("Debug info payload intercepting: " + new String(payload));
        try {
            EnrichedEventBody enrichedEventBody
                    = fromJSONString(new String(payload), EnrichedEventBody.class);

            Map<String, String> exData = enrichedEventBody.getExtraData();
            exData.putAll(service.enrichMap(exData));
            byte[] newPayload = enrichedEventBody.buildEventBody();
            event.setBody(newPayload);
        } catch (IOException e) {
            logger.error(e.getMessage(), e);
        }
        logger.debug("Flume cache Service intercepting: " + new String(event.getBody()));
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> intercepted = new LinkedList<>();
        for (Event e : events) {
            intercepted.add(intercept(e));
        }
        return intercepted;
    }

    ApplicationContext getContext() {
        return springContext;
    }

    public ICacheService<Event> getService() {
        return service;
    }

    @Override
    public void close() {
        logger.debug("Stopping scheduler!!!!!");
        scheduler.shutdown();
    }

    public static class Builder implements Interceptor.Builder {

        private Context flumeContext;

        private ApplicationContext springContext;

        public Interceptor build() {
            return new CacheableInterceptor(flumeContext, springContext);
        }

        @Override
        public void configure(Context context) {
            this.flumeContext = context;
        }

        public void setSpringContext(ApplicationContext springContext) {
            this.springContext = springContext;
        }

    }
}

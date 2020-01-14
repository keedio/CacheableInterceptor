package org.keedio.flume.interceptor.cacheable.service;

import org.keedio.flume.interceptor.cacheable.service.CacheServiceEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Luca Rosellini <lrosellini@keedio.com> on 10/3/15.
 */
@Component
public class CacheEventListener implements ApplicationListener<CacheServiceEvent> {
    private static AtomicInteger counter = new AtomicInteger(0);

    @Override
    public void onApplicationEvent(CacheServiceEvent event) {
        counter.incrementAndGet();
    }

    public int getCounterValue(){
        return counter.intValue();
    }

    public void resetCounter(){
        counter.set(0);
    }
}

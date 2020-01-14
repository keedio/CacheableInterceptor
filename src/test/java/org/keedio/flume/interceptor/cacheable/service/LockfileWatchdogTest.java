package org.keedio.flume.interceptor.cacheable.service;

import org.keedio.flume.interceptor.cacheable.service.ICacheService;
import org.keedio.flume.interceptor.cacheable.service.LockfileWatchdog;
import org.apache.flume.Event;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration("classpath:beans.xml")
public class LockfileWatchdogTest {

    public static final String LOCKFILE = System.getProperty("java.io.tmpdir") +File.separator+ "flumecache.lock";

    @Autowired
    private ICacheService<Event> service;

    @Autowired
    private ApplicationContext springContext;

    @Test
    public void testRun() throws IOException, InterruptedException {

        LockfileWatchdog watcher = new LockfileWatchdog(LOCKFILE, service);

        File lockfile = new File(LOCKFILE);
        lockfile.createNewFile();
        assertTrue(lockfile.exists());

        watcher.run();

        assertFalse(lockfile.exists());
    }

    @Test
    public void testRunNullLockFile() throws IOException, InterruptedException {

        LockfileWatchdog watcher = new LockfileWatchdog(null, service);

        File lockfile = new File(LockfileWatchdog.DEFAULT_LOCKFILE);
        lockfile.createNewFile();
        assertTrue(lockfile.exists());

        watcher.run();

        assertFalse(lockfile.exists());
    }

}

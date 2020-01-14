package org.keedio.flume.interceptor.cacheable.service;

import org.apache.commons.lang3.StringUtils;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

public class LockfileWatchdog implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(LockfileWatchdog.class);

    static final String DEFAULT_LOCKFILE = System.getProperty("java.io.tmpdir") +File.separator+ "flumecache.lock";

    private final ICacheService<Event> cacheService;
    private final String lockFile;

    public LockfileWatchdog(String lockFile, ICacheService<Event> service) {
        this.cacheService = service;

        if (StringUtils.isEmpty(lockFile)){
            logger.warn("\"properties.lock.filename\" not specified, using default lock file: " + DEFAULT_LOCKFILE);
            this.lockFile = DEFAULT_LOCKFILE;
        } else {
            this.lockFile = lockFile;
        }
    }

    @Override
    public void run() {
        File f = new File(lockFile);
        if (f.exists() && !f.isDirectory()) {
            cacheService.evictCache();
            if (!f.delete()) {
                logger.error("Couldn't delete lock file: " + lockFile);
            }
        } else {
            logger.trace("File not found: " + lockFile);
        }
    }
}
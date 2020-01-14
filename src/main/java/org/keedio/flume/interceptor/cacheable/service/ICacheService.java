package org.keedio.flume.interceptor.cacheable.service;

import java.io.IOException;
import java.util.Map;

public interface ICacheService<T> {
    String PROPERTIES_CSV_SEPARATOR = "properties.csv.separator";
    String PROPERTIES_CSV_QUOTE_CHAR = "properties.csv.quote.char";
    String PROPERTIES_CSV_DIRECTORY = "properties.csv.directory";
    String PROPERTIES_LOCKFILE = "properties.lock.filename";
    String PROPERTIES_WATCH_INTERVAL = "properties.lock.interval";

    String DEFAULT_CSV_SEPARATOR = ",";
    String DEFAULT_CSV_QUOTE_CHAR = "\"";
    String PROPERTIES_CRITERIA_SELECTION = "properties.selection.criteria.";


    public void setProperties(Map<String, String> props);

    /*
        @return Map
        @param String to enrich data according key named 'hostname'.
        @param data to be enriched
        */
    //@Cacheable(value = "FlumeCachedEvent")
    Map<String, String> enrichMap(Map<String, String> data) throws IOException;

    /**
     * Utility method to evict the cache. Implementation should use the "@CacheEvict" annotation.
     */
    public void evictCache();
}

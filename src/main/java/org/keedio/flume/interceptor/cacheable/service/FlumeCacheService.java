package org.keedio.flume.interceptor.cacheable.service;

import com.opencsv.CSVReader;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flume.Event;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationEvent;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.zip.GZIPInputStream;

import org.keedio.flume.interceptor.cacheable.interceptor.CriteriaFilter;
import com.google.common.collect.Maps;
import static org.apache.commons.lang.Validate.isTrue;
import static org.apache.commons.lang.Validate.notEmpty;
import static org.apache.commons.lang3.StringUtils.isEmpty;

class CacheServiceEvent extends ApplicationEvent {

    /**
     * Create a new ApplicationEvent.
     *
     * @param source the component that published the event (never {@code null})
     */
    public CacheServiceEvent(Object source) {
        super(source);
    }
}

@Component
public class FlumeCacheService implements ICacheService<Event> {

    private static final Logger logger = LoggerFactory.getLogger(FlumeCacheService.class);

    private char csvSeparator;
    private char quoteChar;
    private Path startingDir;
    private Map<String, String> criterias = new HashMap<>();
    private CriteriaFilter criteriaFilter = new CriteriaFilter();
    private List<CriteriaFilter> listCriterias = new ArrayList<>();

    private static final CacheServiceEvent EVENT = new CacheServiceEvent("FlumeCacheService");

    @Autowired
    private ApplicationContext context;

    /**
     * Generic way to pass parameters from the Flume interceptor
     *
     * @param props
     */
    @Override
    public void setProperties(Map<String, String> props) {
        String separator = props.get(PROPERTIES_CSV_SEPARATOR);
        String quoteChar = props.get(PROPERTIES_CSV_QUOTE_CHAR);

        if (isEmpty(separator)) {
            logger.warn("property " + PROPERTIES_CSV_SEPARATOR + " not found. Using comma as separator");
            this.csvSeparator = DEFAULT_CSV_SEPARATOR.charAt(0);
        } else {
            isTrue(separator.length() == 1, "Separator length must be a single character!");
            this.csvSeparator = separator.charAt(0);
        }

        if (isEmpty(quoteChar)) {
            logger.warn("property " + PROPERTIES_CSV_QUOTE_CHAR + " not found. Using \" as quote char");
            this.quoteChar = DEFAULT_CSV_QUOTE_CHAR.charAt(0);
        } else {
            isTrue(separator.length() == 1, "Separator length must be a single character!");
            this.quoteChar = quoteChar.charAt(0);
        }
        
        String directory = props.get(PROPERTIES_CSV_DIRECTORY);

        notEmpty(directory);
        startingDir = Paths.get(directory);

        isTrue(startingDir.toFile().exists());
        isTrue(startingDir.toFile().isDirectory());

        criterias.putAll(props);
        criterias.remove(PROPERTIES_CSV_SEPARATOR);
        criterias.remove(PROPERTIES_CSV_DIRECTORY);
        criterias.remove(PROPERTIES_CSV_QUOTE_CHAR);
        
        try {
            listCriterias = criteriaFilter.createListCriteria(criterias);
        } catch (IOException e) {
            logger.error("IO", e);
        }
    }

    /**
     * Method that enriches the given input data Map using a set of CSV files
     * provided in specific folder.
     * <p/>
     * When a match is found in one of the CSV files, the search stops and the
     * additional properties are returned.
     *
     * @param data a map containing the keys used to search for a record in the
     * input CSV files.
     * @return a map containing the enriched data.
     * @throws IOException
     */
    @Cacheable(value = "FlumeCachedEvent", keyGenerator = "mapKeyGenerator")
    @Override
    public Map<String, String> enrichMap(final Map<String, String> data) throws IOException {
        context.publishEvent(EVENT);

        final Map<String, String> enrichedMap = new HashMap<>();

        Files.walkFileTree(startingDir, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                String filename = file.toString().trim().toLowerCase();
                if (filename.endsWith(".csv") || filename.endsWith(".csv.gz")) {

                    Map<String, String> additionalProps = findAdditionalPropsInCsv(file.toString(), data);

                    if (!additionalProps.isEmpty()) {
                        enrichedMap.putAll(additionalProps);
                        return FileVisitResult.TERMINATE;
                    }
                }
                return FileVisitResult.CONTINUE;
            }
        });
        return enrichedMap;
    }

    /**
     * Evict the cache and delete the lock file used as trigger
     */
    @Override
    @CacheEvict(value = "FlumeCachedEvent", allEntries = true, beforeInvocation = true)
    public void evictCache() {
        logger.debug("Evicting cache!");
    }

    private Reader initReader(String externalFile) throws IOException {
        if (externalFile.endsWith(".gz")){
            return new InputStreamReader(new GZIPInputStream(new FileInputStream(externalFile)));
        } else {
            return new FileReader(externalFile);
        }
    } 
    
    /**
     * Scans the given csv files for a match for the given key data.
     *
     * @param externalFile the external csv file to search for.
     * @param data the map data used as the search key.
     * @return the map of values to be used to enrich the event.
     */
    private Map<String, String> findAdditionalPropsInCsv(String externalFile, Map<String, String> data) {
        Map<String, String> additionalFields = new HashMap<>();
        try {
            long startReading = System.currentTimeMillis();
            try (CSVReader reader = new CSVReader(
                    initReader(externalFile), csvSeparator, quoteChar)) {
                // header row
                String[] firstRow = reader.readNext();
                String[] currRow;
                Set<String> keySet = data.keySet();
                int count = 0;
                while ((currRow = reader.readNext()) != null) {
                    if (currRow.length == firstRow.length) {
                        Pair<Map<String, String>, Map<String, String>> keysAndValues
                                = buildKeysAndValues(firstRow, currRow, keySet);

                        Map<String, String> filteredData = new HashMap<>();
                        
                        for (Map.Entry<String, String> item : keysAndValues.getLeft().entrySet()){
                            if (data.keySet().contains(item.getKey())){
                                filteredData.put(item.getKey(), data.get(item.getKey()));
                            }
                        }

                        if (keysAndValues.getLeft().equals(filteredData)) {
                            if (!listCriterias.isEmpty()) {
                                additionalFields = filterFieldsCsv(keysAndValues, listCriterias);
                            } else {
                                additionalFields = keysAndValues.getRight(); //no apply filter
                            }
                            return additionalFields;
                        }
                    } else {
                        String msg = StringUtils.join(currRow,'|');
                        
                       logger.error("Line number: " + count + " the number of records is not equal to number of header fields. Found: " + currRow.length + ", expected: " + firstRow.length + ". currRow fields: " + msg );
                    }
                    count++;
                }
            } finally {
                long elapsedMilsec = System.currentTimeMillis() - startReading;
                logger.info("Elapsed time (ms): " + elapsedMilsec + " filename in process :  " + externalFile);
            }
        } catch (FileNotFoundException e) {
            logger.error("FileNotFoundException", e);
        } catch (IOException e) {
            logger.error("IOException", e);
        }

        return additionalFields;
    }

    /**
     * Given the header of the input CSV file, a generic CSV row and a the set
     * of CSV key names, for the generic Nth row of the CSV files, maps the key
     * names and value name to the corresponding values of the generic row.
     * <p/>
     * Example: keySet: {FunctionalEnvironment,Delivery} headerRow:
     * {FunctionalEnvironment,CI Name,CI ID,Delivery} currentRow:
     * {"Production","***REMOVED***","***REMOVED***","***REMOVED***"}
     * <p/>
     * returns the following map of keys:
     * {FunctionalEnvironment="Production",Delivery="***REMOVED***"}
     * <p/>
     * and the following map of values: {CI Name="***REMOVED***",CI
     * ID="***REMOVED***"}
     *
     * @param headerRow csv header row.
     * @param currRow Nth row of the CSV file.
     * @param keySet the set of key names.
     * @return see above.
     */
    private Pair<Map<String, String>, Map<String, String>> buildKeysAndValues(
            String[] headerRow, String[] currRow, Set<String> keySet) {

        Map<String, String> keys = new HashMap<>();
        Map<String, String> values = new HashMap<>();

        for (int i = 0; i < headerRow.length; i++) {
            String candidate = headerRow[i].trim();

            if (keySet.contains(candidate)) {
                keys.put(candidate, currRow[i]);
            } else {
                values.put(candidate, currRow[i]);
            }
        }
        return Pair.of(keys, values);
    }

    /**
     * Filter fiedls in CSV according list of selected criteria
     *
     * @param pair full csv as Pair object
     * @param list criteria selection's fields
     * @return new map of additionalfields if match criteria
     */
    public Map<String, String> filterFieldsCsv(Pair<Map<String, String>, Map<String, String>> pair, List<CriteriaFilter> list) {
        Map<String, String> result = new HashMap<>();
        Map<String, String> completeRow = new HashMap<>();
        Map<String, String> commonMap = new HashMap<>();
        completeRow.putAll(pair.getLeft());
        completeRow.putAll(pair.getRight());

        for (CriteriaFilter criterio : list) {
            commonMap = Maps.difference(completeRow, criterio.getKey()).entriesInCommon();
            if (criterio.getKey().equals(commonMap)) {
                for (String field : criterio.getValues()) {
                    result.put(field, completeRow.get(field));
                }
                break;
            } else {
                result = pair.getRight();
            }
        }
        return result;
    }

}

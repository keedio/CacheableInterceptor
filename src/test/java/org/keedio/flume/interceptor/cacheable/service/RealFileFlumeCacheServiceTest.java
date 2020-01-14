package org.keedio.flume.interceptor.cacheable.service;

import org.keedio.flume.interceptor.cacheable.service.ICacheService;
import com.google.common.collect.ImmutableMap;
import junit.framework.Assert;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flume.Event;
import org.junit.Ignore;
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

@Ignore
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = {"classpath:beans.xml"})
public class RealFileFlumeCacheServiceTest {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(RealFileFlumeCacheServiceTest.class);

    @Autowired
    private ICacheService<Event> service;

    @PostConstruct
    public void init() {
        service.setProperties(ImmutableMap.of(
                ICacheService.PROPERTIES_CSV_SEPARATOR, "|",
                ICacheService.PROPERTIES_CSV_DIRECTORY, "/files/csv"));
    }

    @Test
    public void testEnrichMapMultipleKeys() throws IOException {
        System.out.println("enrichMap");
        Pair<Map<String, String>, Map<String, String>> dt = prepareData();

        Map<String, String> mapResult = service.enrichMap(dt.getRight());

        org.junit.Assert.assertEquals(dt.getLeft(), mapResult);
    }

    private Pair<Map<String, String>, Map<String, String>> prepareData() {
        Map<String, String> data = new HashMap<String, String>();
        Map<String, String> data2 = new HashMap<String, String>();
        data.put("Company", "Contoso");
        data.put("VirtualDC", "Contoso ES");
        data.put("FQDN ADM", "svc1-adm.contoso.corp");
        data.put("Manufacturer", "");
        data.put("IPAdmin", "11.10.9.8");
        data.put("Network Distribution","SERVIDORES INTRANET");
        data.put("Technical SVC CI ID","***REMOVED***");
        data.put("Technical SVC CI Name","INTRANET BKS");
        data.put("Category","Logical Systems");
        data.put("Business SVC CI ID","***REMOVED***");
        data.put("Business SVC CI Name","INTRANET PRIVADA");
        data.put("Model/Version","");
        data.put("Item","Authentication");
        data.put("Function","LDAP");
        data.put("IP Address","22.10.9.8 122.110.9.8");
        data.put("Operating System","Solaris 10");
        data.put("Status","Deployed");
        data.put("Priority","***REMOVED***");
        data.put("Product Name","");
        data.put("Hostname","svc1");
        data.put("Type","Logical Server");

        data2.put("FunctionalEnvironment","Production");
        data2.put("CI Name", "svc1");
        data2.put("CI ID", "LServer_0000001");
        data2.put("Delivery", "DC1");

        return Pair.of(data, data2);
    }

    @Test
    public void testEnrichMapMultipleKeysNoMatch() throws IOException {
        System.out.println("enrichMap");
        Pair<Map<String, String>, Map<String, String>> dt = prepareData();

        dt.getRight().put("Delivery","DC1");

        Map<String, String> mapResult = service.enrichMap(dt.getRight());

        org.junit.Assert.assertTrue(mapResult.isEmpty());
    }
}

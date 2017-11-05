package ar.edu.itba.pod.client;

import ar.edu.itba.pod.api.OrderStringIntMapCollator;
import ar.edu.itba.pod.example.TokenizerMapper;
import ar.edu.itba.pod.example.WordCountReducerFactory;

import ar.edu.itba.pod.model.Data;

import ar.edu.itba.pod.model.DepartmentNameOcurrenciesCount;

import ar.edu.itba.pod.model.DepartmentPairOcurrenciesCount;
import ar.edu.itba.pod.query1.ProvinceRegionMapper;
import ar.edu.itba.pod.query1.ProvinceRegionReducerFactory;
import ar.edu.itba.pod.query3.UnemploymentIndexCollator;
import ar.edu.itba.pod.query3.UnemploymentIndexMapper;
import ar.edu.itba.pod.query3.UnemploymentIndexReducerFactory;
import ar.edu.itba.pod.query4.HogarCountMapper;
import ar.edu.itba.pod.query4.HogarCountReducerFactory;
import ar.edu.itba.pod.query6.DepartmentCollator;
import ar.edu.itba.pod.query6.DepartmentMapper;
import ar.edu.itba.pod.query6.DepartmentReducerFactory;
import ar.edu.itba.pod.query2.Query2CountCollator;
import ar.edu.itba.pod.query5.Query5Collator;
import ar.edu.itba.pod.query5.Query5Combiner;
import ar.edu.itba.pod.query5.Query5Mapper;
import ar.edu.itba.pod.query5.Query5ReducerFactory;
import ar.edu.itba.pod.query7.*;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;

import ar.edu.itba.pod.query2.Query2CountReducerFactory;
import ar.edu.itba.pod.query2.Query2Mapper;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;

import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);
    private static IMap<String, String> booksMap;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        logger.info("pod-map-reduce Client Starting ...");

        final ClientConfig config = new ClientConfig();
        final HazelcastInstance hz = HazelcastClient.newHazelcastClient(config);

        query1(hz, "census100.csv");
        query2(hz, "census100.csv", "Buenos Aires", 2);
        query3(hz, "census100.csv");
        query4(hz, "census100.csv");
        query5(hz, "census100.csv");
        query6(hz, "census100.csv", 2);
        query7(hz, "census100.csv", 2);
        query7v2(hz, "census100.csv", 1);

    }

    private static void logQuery6(Map<String, DepartmentNameOcurrenciesCount> result) {
        /* Print by lines
        for (DepartmentNameOcurrenciesCount department : result.values()) {
            logger.info(department.toString());
        }
        */
        logger.info("RESULTS: " + result.toString());
    }

    /* *********************************************************** */
    /* ************************* TEST Qy ************************* */
    /* *********************************************************** */

    public static IMap<String,String> getBooksMap(HazelcastInstance client) {
        IMap<String,String> booksMap = client.getMap("books");

        booksMap.put("Dracula","  3 May. Bistriz.- Left Munich at 8:35 P.M., on 1st May, arriving at");
        booksMap.put("Dracula","Vienna early next morning; should have arrived at 6:46, but train");
        booksMap.put("MobyDick", "This text of Melville's Moby-Dick is based on the Hendricks House edition.");
        booksMap.put("MobyDick", "It was prepared by Professor Eugene F. Irey at the University of Colorado.");

        return booksMap;
    }
    public static void testQuery(HazelcastInstance hz) throws ExecutionException, InterruptedException {
        JobTracker jobTracker = hz.getJobTracker("word-count");

        IMap<String,String> map = getBooksMap(hz);
        //Source es un wrapper para IMap.
        final KeyValueSource<String, String> source = KeyValueSource.fromMap(map);

        Job<String, String> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new TokenizerMapper())
                .reducer(new WordCountReducerFactory())
                .submit();

        Map<String, Long> result = future.get();
        logger.info("RESULTS: "+result.toString());
    }

    /* *********************************************************** */
    /* ************************* QUERY 1 ************************* */
    /* *********************************************************** */

    private static void query1(HazelcastInstance hz, String fileName) {

        JobTracker jobTracker = hz.getJobTracker("provinceRegion");

        IList<Data> list = getQuery1List(hz, fileName);
        final KeyValueSource<String, Data> source = KeyValueSource.fromList( list );


        Job<String, Data> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Integer>> future = job
                .mapper(new ProvinceRegionMapper())
                .reducer(new ProvinceRegionReducerFactory())
                .submit(new OrderStringIntMapCollator()); // adentro del submit recibe un collator para ordenar

        Map<String, Integer> result = null;
        try {
            result = future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        logger.info("RESULTS: "+result.toString());

    }

    private static IList<Data> getQuery1List(HazelcastInstance client, String fileName) {
        IList<Data> list = client.getList("provinceRegion");
        list.clear();
        DataReader.readToList(list, fileName);
        return list;
    }

    /* *********************************************************** */
    /* ************************* QUERY 2 ************************* */
    /* *********************************************************** */

    private static void query2(HazelcastInstance hz, String fileName, String provinceName, int n) {
        JobTracker jobTracker = hz.getJobTracker("departmentCount");

        IList<Data> list = getQuery2List(hz, fileName, provinceName);

        final KeyValueSource<String, Data> source = KeyValueSource.fromList(list);
        Job<String, Data> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new Query2Mapper())
                .reducer(new Query2CountReducerFactory())
                .submit(new Query2CountCollator(n));

        Map<String, Long> result = null;
        // TODO --> Check what to do with these exceptions
        try {
            result = future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        logger.info("RESULTS: "+result.toString());
    }

    private static IList<Data> getQuery2List(HazelcastInstance client, String fileName, String provinceName) {
        IList<Data> list = client.getList("departments");
        list.clear();
        DataReader.readToList(list, fileName);
        // FIXME - This could be replaced with a Predicate, but don't know how to do that (because is on the value, not the key)
        for (Data d : list) {
            if (!d.getProvinceName().toLowerCase().equals(provinceName.toLowerCase()))
                list.remove(d);
        }
        return list;
    }

    /* *********************************************************** */
    /* ************************* QUERY 3 ************************* */
    /* *********************************************************** */

    private static void query3(HazelcastInstance hz, String fileName) {

        JobTracker jobTracker = hz.getJobTracker("UnemploymentIndex");

        IList<Data> list = getQuery3List(hz, fileName);
        final KeyValueSource<String, Data> source = KeyValueSource.fromList( list );


        Job<String, Data> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, BigDecimal>> future = job
                .mapper(new UnemploymentIndexMapper())
                .reducer(new UnemploymentIndexReducerFactory())
                .submit(new UnemploymentIndexCollator()); // adentro del submit recibe un collator para ordenar


        Map<String, BigDecimal> result = null;
        try {
            result = future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        logger.info("RESULTS: "+result.toString());

    }

    private static IList<Data> getQuery3List(HazelcastInstance client, String fileName) {
        IList<Data> list = client.getList("UnemploymentIndex");
        list.clear();
        DataReader.readToList(list, fileName);
        return list;
    }

    /* *********************************************************** */
    /* ************************* QUERY 4 ************************* */
    /* *********************************************************** */

    private static void query4(HazelcastInstance hz, String fileName) {

        JobTracker jobTracker = hz.getJobTracker("HogarCount");

        IList<Data> list = getQuery4List(hz, fileName);
        final KeyValueSource<String, Data> source = KeyValueSource.fromList( list );


        Job<String, Data> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Integer>> future = job
                .mapper(new HogarCountMapper())
                .reducer(new HogarCountReducerFactory())
                .submit(new OrderStringIntMapCollator()); // adentro del submit recibe un collator para ordenar

        Map<String, Integer> result = null;
        try {
            result = future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        logger.info("RESULTS: "+result.toString());

    }

    private static IList<Data> getQuery4List(HazelcastInstance client, String fileName) {
        IList<Data> list = client.getList("HogarCount");
        list.clear();
        DataReader.readToList(list, fileName);
        return list;
    }

    /* *********************************************************** */
    /* ************************* QUERY 5 ************************* */
    /* *********************************************************** */

    private static void query5(HazelcastInstance hz, String fileName) {
        JobTracker jobTracker = hz.getJobTracker("regionAverage");

        IList<Data> list = getQuery5List(hz, fileName);

        final KeyValueSource<String, Data> source = KeyValueSource.fromList(list);
        Job<String, Data> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Float>> future = job
                .mapper(new Query5Mapper())
                .combiner(new Query5Combiner())
                .reducer(new Query5ReducerFactory())
                .submit(new Query5Collator());

        Map<String, Float> result = null;
        // TODO --> Check what to do with these exceptions
        try {
            result = future.get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        logger.info("RESULTS: "+result.toString());
    }
    private static IList<Data> getQuery5List(HazelcastInstance client, String fileName) {
        IList<Data> list = client.getList("regionAvg");
        list.clear();
        DataReader.readToList(list, fileName);
        return list;
    }

    /* *********************************************************** */
    /* ************************* QUERY 6 ************************* */
    /* *********************************************************** */

    private static void query6(HazelcastInstance hz, String fileName, int n) {

        JobTracker jobTracker = hz.getJobTracker("departmentInProvince");

        IList<Data> list = getQuery6List(hz, fileName);
        final KeyValueSource<String, Data> source = KeyValueSource.fromList( list );


        Job<String, Data> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, DepartmentNameOcurrenciesCount>> future = job
                .mapper(new DepartmentMapper())
                .reducer(new DepartmentReducerFactory())
                .submit(new DepartmentCollator(n)); // adentro del submit recibe un collator para ordenar

        try {
            Map<String, DepartmentNameOcurrenciesCount> result = future.get();
            logQuery6(result);
        } catch (InterruptedException e1) {

        } catch (ExecutionException e) {

        }
    }

    private static IList<Data> getQuery6List(HazelcastInstance client, String fileName) {
        IList<Data> list = client.getList("departmentInProvince");
        list.clear();
        DataReader.readToList(list, fileName);
        return list;
    }

    /* *********************************************************** */
    /* ************************* QUERY 7 ************************* */
    /* *********************************************************** */

    private static void query7(HazelcastInstance hz, String fileName, int n) {

        JobTracker jobTracker = hz.getJobTracker("provincePairInDepartment");

        IList<Data> list = getQuery7List(hz, fileName);
        final KeyValueSource<String, Data> source = KeyValueSource.fromList( list );


        Job<String, Data> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, DepartmentPairOcurrenciesCount>> future = job
                .mapper(new DepartmentInProvinceMapper())
                .combiner(new DeparmentPairCombinerFactory())
                .reducer(new DepartmentPairReducerFactory())
                .submit(new DepartmentPairCollator(1)); // adentro del submit recibe un collator para ordenar

        try {
            Map<String, DepartmentPairOcurrenciesCount> result = future.get();
            for (DepartmentPairOcurrenciesCount department : result.values()) {
                logger.info(department.toString());
            }
        } catch (InterruptedException e1) {

        } catch (ExecutionException e) {

        }
    }

    private static IList<Data> getQuery7List(HazelcastInstance client, String fileName) {
        IList<Data> list = client.getList("provincePairInDepartment");
        list.clear();
        DataReader.readToList(list, fileName);
        return list;
    }

    /* *********************************************************** */
    /* ************************* QUERY 7v2 ************************* */
    /* *********************************************************** */

    private static void query7v2(HazelcastInstance hz, String fileName, int n) {

        JobTracker jobTracker = hz.getJobTracker("provincePairInDepartment");

        IList<Data> list = getQuery7v2List(hz, fileName);
        final KeyValueSource<String, Data> source = KeyValueSource.fromList( list );


        Job<String, Data> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Map<String, String>>> future = job
                .mapper(new DepartmentInProvinceMapper())
                .combiner(new DeparmentPairCombinerFactory())
                .reducer(new DepartmentPairReducerFactory())
                .submit(); // adentro del submit recibe un collator para ordenar

        try {
            Map<String, Map<String, String>> result = future.get();
            IMap<String, Map<String, String>> imap = hz.getMap("provincePairInDepartment");
            for (String key : result.keySet()){
                imap.put(key, result.get(key));
            }
            final KeyValueSource<String, Map<String, String>> source1 = KeyValueSource.fromMap( imap );
            Job<String, Map<String, String>> job1 = jobTracker.newJob(source1);
            ICompletableFuture<Map<String, Integer>> future1 = job1
                    .mapper(new ProvPairMapper())
                    .reducer(new ProvPairReducerFactory())
                    .submit(new ProvPairCollator(n));
            Map<String, Integer> result1 = future1.get();
            for (String pairprov : result1.keySet()) {
                logger.info(pairprov +","+ result1.get(pairprov));
            }
        } catch (InterruptedException e1) {

        } catch (ExecutionException e) {

        }
    }

    private static IList<Data> getQuery7v2List(HazelcastInstance client, String fileName) {
        IList<Data> list = client.getList("provincePairInDepartment");
        list.clear();
        DataReader.readToList(list, fileName);
        return list;
    }
}

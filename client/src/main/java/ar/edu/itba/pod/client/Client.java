package ar.edu.itba.pod.client;

import ar.edu.itba.pod.client.model.InputData;
import ar.edu.itba.pod.client.util.CommandLineUtil;
import ar.edu.itba.pod.example.TokenizerMapper;
import ar.edu.itba.pod.example.WordCountReducerFactory;

import ar.edu.itba.pod.model.Data;
//import ar.edu.itba.pod.query4.HogarCountCollator;
//import ar.edu.itba.pod.query4.HogarCountMapper;
//import ar.edu.itba.pod.query4.HogarCountReducerFactory;

import ar.edu.itba.pod.query2.Query2CountCollator;

import ar.edu.itba.pod.query5.Query5Collator;
import ar.edu.itba.pod.query5.Query5Combiner;
import ar.edu.itba.pod.query5.Query5Mapper;
import ar.edu.itba.pod.query5.Query5ReducerFactory;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;

import ar.edu.itba.pod.query2.Query2CountReducerFactory;
import ar.edu.itba.pod.query2.Query2Mapper;

import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;

import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);
    private static IMap<String, String> booksMap;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        logger.info("pod-map-reduce Client Starting ...");

        InputData input = CommandLineUtil.getInputData(args);

        logger.info(String.format("Connecting with cluster [%s]", input.getClusterName()));

        ClientConfig clientConfig = new ClientConfig();
        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();
        for (String address : input.getAddresses()){
            networkConfig.addAddress(address);
        }
        clientConfig.getGroupConfig().setName(input.getClusterName()).setPassword(input.getClusterPass());

        HazelcastInstance hz = HazelcastClient.newHazelcastClient(clientConfig);

        query2(hz, input.getInPath(), input.getProvince(), input.getN());

    }

    /* *********************************************************** */
    /* ************************* TEST Qy ************************* */
    /* *********************************************************** */

    public static IMap<String, String> getBooksMap(HazelcastInstance client) {
        IMap<String, String> booksMap = client.getMap("books");

        booksMap.put("Dracula", "  3 May. Bistriz.- Left Munich at 8:35 P.M., on 1st May, arriving at");
        booksMap.put("Dracula", "Vienna early next morning; should have arrived at 6:46, but train");
        booksMap.put("MobyDick", "This text of Melville's Moby-Dick is based on the Hendricks House edition.");
        booksMap.put("MobyDick", "It was prepared by Professor Eugene F. Irey at the University of Colorado.");

        return booksMap;
    }

    public static void testQuery(HazelcastInstance hz) throws ExecutionException, InterruptedException {
        JobTracker jobTracker = hz.getJobTracker("word-count");

        IMap<String, String> map = getBooksMap(hz);
        //Source es un wrapper para IMap.
        final KeyValueSource<String, String> source = KeyValueSource.fromMap(map);

        Job<String, String> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new TokenizerMapper())
                .reducer(new WordCountReducerFactory())
                .submit();

        Map<String, Long> result = future.get();
        logger.info("RESULTS: " + result.toString());
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

        logger.info("RESULTS: " + result.toString());
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

        logger.info("RESULTS: " + result.toString());
    }

    private static IList<Data> getQuery5List(HazelcastInstance client, String fileName) {
        IList<Data> list = client.getList("regionAvg");
        list.clear();
        DataReader.readToList(list, fileName);
        return list;
    }


}

//    Config cfg = new Config();
//cfg.setPort(5900);
//cfg.setPortAutoIncrement(false);
//
//    NetworkConfig network = cfg.getNetworkConfig();
//    Join join = network.getJoin();
//join.getMulticastConfig().setEnabled(false);
//join.getTcpIpConfig().addMember("10.45.67.32").addMember("10.45.67.100")
//            .setRequiredMember("192.168.10.100").setEnabled(true);
//network.getInterfaces().setEnabled(true).addInterface("10.45.67.*");
//
//    MapConfig mapCfg = new MapConfig();
//mapCfg.setName("testMap");
//mapCfg.setBackupCount(2);
//mapCfg.getMaxSizeConfig().setSize(10000);
//mapCfg.setTimeToLiveSeconds(300);
//
//    MapStoreConfig mapStoreCfg = new MapStoreConfig();
//mapStoreCfg.setClassName("com.hazelcast.examples.DummyStore").setEnabled(true);
//mapCfg.setMapStoreConfig(mapStoreCfg);
//
//    NearCacheConfig nearCacheConfig = new NearCacheConfig();
//nearCacheConfig.setMaxSize(1000).setMaxIdleSeconds(120).setTimeToLiveSeconds(300);
//mapCfg.setNearCacheConfig(nearCacheConfig);
//
//cfg.addMapConfig(mapCfg)
//

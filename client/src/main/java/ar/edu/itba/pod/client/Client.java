package ar.edu.itba.pod.client;

import ar.edu.itba.pod.example.TokenizerMapper;
import ar.edu.itba.pod.example.WordCountReducerFactory;

import ar.edu.itba.pod.model.Data;
import ar.edu.itba.pod.query1.ProvinceRegionCollator;
import ar.edu.itba.pod.query1.ProvinceRegionMapper;
import ar.edu.itba.pod.query1.ProvinceRegionReducerFactory;
//import ar.edu.itba.pod.query4.HogarCountCollator;
//import ar.edu.itba.pod.query4.HogarCountMapper;
//import ar.edu.itba.pod.query4.HogarCountReducerFactory;


import ar.edu.itba.pod.query2.Query2CountCollator;


import ar.edu.itba.pod.query2.Query2CountCollator;

import ar.edu.itba.pod.query2.Query2CountReducerFactory;
import ar.edu.itba.pod.query2.Query2Mapper;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.*;

import ar.edu.itba.pod.query2.Query2CountReducerFactory;
import ar.edu.itba.pod.query2.Query2Mapper;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.ICompletableFuture;
import com.hazelcast.core.IList;
import com.hazelcast.core.IMap;

import com.hazelcast.core.*;

import com.hazelcast.mapreduce.Job;
import com.hazelcast.mapreduce.JobTracker;
import com.hazelcast.mapreduce.KeyValueSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;

import java.util.ArrayList;
import java.util.Collection;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import java.util.ArrayList;
import java.util.Collection;

import java.util.Map;
import java.util.concurrent.ExecutionException;

public class Client {
    private static Logger logger = LoggerFactory.getLogger(Client.class);
    private static IMap<String, String> booksMap;

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        logger.info("pod-map-reduce Client Starting ...");

        final ClientConfig config = new ClientConfig();
        final HazelcastInstance hz = HazelcastClient.newHazelcastClient(config);

        /*  query example
        JobTracker jobTracker = hz.getJobTracker("word-count");
        IMap<String,String> map = getBooksMap(hz);
        Source es un wrapper para IMap.
        final KeyValueSource<String, String> source = KeyValueSource.fromMap(map);


        Job<String, String> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Long>> future = job
                .mapper(new TokenizerMapper())
                .reducer(new WordCountReducerFactory())
                .submit();
                */

        /* //query 1

        final IList<Data> list = hz.getList( "my-list" );
        DataReader.readToList(list, "/Users/agophurmuz/Downloads/census100.csv");
        final KeyValueSource<String, Data> source = KeyValueSource.fromList( list );

        Job<String, Data> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Integer>> future = job
                .mapper(new ProvinceRegionMapper())
                .reducer(new ProvinceRegionReducerFactory())
                .submit(new ProvinceRegionCollator()); // adentro del submit recibe un collator para ordenar
         */

        /*/ query 4
        final IList<Data> list = hz.getList( "my-list" );
        list.clear();
        DataReader.readToList(list, "/Users/agophurmuz/Downloads/census100.csv");
        final KeyValueSource<String, Data> source = KeyValueSource.fromList( list );

        Job<String, Data> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Integer>> future = job
                .mapper(new HogarCountMapper())
                .reducer(new HogarCountReducerFactory())
                .submit(new HogarCountCollator()); // adentro del submit recibe un collator para ordenar

        Map<String, Integer> result = future.get();

        logger.info("RESULTS: "+result.toString());
        */

        query2(hz, "census100.2.csv", "Buenos Aires", 2);
    }

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
    /* ************************* QUERY 2 ************************* */
    /* *********************************************************** */

    private static void query2(HazelcastInstance hz, String fileName, String provinceName, int n) {
        JobTracker jobTracker = hz.getJobTracker("departmentCount");

        IMap<String,Long> map = getQuery2Map(hz, fileName, provinceName);

        final KeyValueSource<String, Long> source = KeyValueSource.fromMap(map);
        Job<String, Long> job = jobTracker.newJob(source);
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

    private static IMap<String, Long> getQuery2Map(HazelcastInstance client, String fileName, String provinceName) {
        IMap<String, Long> provinceDepartments = client.getMap(provinceName.concat("Departments"));
        provinceDepartments.clear();

        BufferedReader br;
        String line = "";
        String cvsSplitBy = ",";
        // FIXME check this --> How does the resources folder work
        ClassLoader classLoader = Client.class.getClassLoader();
        String csvFile = classLoader.getResource("census/"+fileName).getPath();
        try {
            br = new BufferedReader(new FileReader(csvFile));
            while ((line = br.readLine()) != null) {
                String[] csvLine = line.split(cvsSplitBy);
                if (csvLine[3].equals(provinceName)) {
                    if (!provinceDepartments.containsKey(csvLine[2]))
                        provinceDepartments.put(csvLine[2], new Long(0));
                    Long aux = provinceDepartments.get(csvLine[2]) + 1;
                    provinceDepartments.put(csvLine[2], aux);
                }
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return provinceDepartments;
    }
}

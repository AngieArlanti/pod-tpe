package ar.edu.itba.pod.client;

import ar.edu.itba.pod.example.WordCountReducerFactory;
import ar.edu.itba.pod.model.Data;
import ar.edu.itba.pod.model.DepartmentNameOcurrenciesCount;
import ar.edu.itba.pod.query1.ProvinceRegionCollator;
import ar.edu.itba.pod.query1.ProvinceRegionMapper;
import ar.edu.itba.pod.query1.ProvinceRegionReducerFactory;
/*import ar.edu.itba.pod.query4.HogarCountCollator;
import ar.edu.itba.pod.query4.HogarCountMapper;
import ar.edu.itba.pod.query4.HogarCountReducerFactory;*/
import ar.edu.itba.pod.query6.DepartmentCollator;
import ar.edu.itba.pod.query6.DepartmentMapper;
import ar.edu.itba.pod.query6.DepartmentReducerFactory;
import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
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

        final ClientConfig config = new ClientConfig();
        final HazelcastInstance hz = HazelcastClient.newHazelcastClient(config);

        JobTracker jobTracker = hz.getJobTracker("word-count");

        /*  query example

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

         // query 4
        /*final IList<Data> list = hz.getList( "my-list" );
        list.clear();
        DataReader.readToList(list, "/Users/agophurmuz/Downloads/census100.csv");
        final KeyValueSource<String, Data> source = KeyValueSource.fromList( list );

        Job<String, Data> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, Integer>> future = job
                .mapper(new HogarCountMapper())
                .reducer(new HogarCountReducerFactory())
                .submit(new HogarCountCollator()); // adentro del submit recibe un collator para ordenar

        Map<String, Integer> result = future.get();

        logger.info("RESULTS: "+result.toString());*/


         //  query 6
        
        final IList<Data> list = hz.getList( "my-list" );
        list.clear();
        DataReader.readToList(list, "/Users/mminestrelli/Downloads/census100.csv");
        final KeyValueSource<String, Data> source = KeyValueSource.fromList( list );


        Job<String, Data> job = jobTracker.newJob(source);
        ICompletableFuture<Map<String, DepartmentNameOcurrenciesCount>> future = job
                .mapper(new DepartmentMapper())
                .reducer(new DepartmentReducerFactory())
                .submit(new DepartmentCollator(1)); // adentro del submit recibe un collator para ordenar

        Map<String, DepartmentNameOcurrenciesCount> result = future.get();

        logQuery6(result);
        //logger.info(result.toString());


    }

    private static void logQuery6(Map<String, DepartmentNameOcurrenciesCount> result) {
        for (DepartmentNameOcurrenciesCount department: result.values()) {
            logger.info(department.toString());
        }
    }

    public static IMap<String,String> getBooksMap(HazelcastInstance client) {
        IMap<String,String> booksMap = client.getMap("books");

        booksMap.put("Dracula","  3 May. Bistriz.- Left Munich at 8:35 P.M., on 1st May, arriving at");
        booksMap.put("Dracula","Vienna early next morning; should have arrived at 6:46, but train");
        booksMap.put("MobyDick", "This text of Melville's Moby-Dick is based on the Hendricks House edition.");
        booksMap.put("MobyDick", "It was prepared by Professor Eugene F. Irey at the University of Colorado.");

        return booksMap;
    }
}

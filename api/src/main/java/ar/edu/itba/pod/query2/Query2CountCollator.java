package ar.edu.itba.pod.query2;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.mapreduce.Collator;

import java.util.*;

/**
 * Created by sebastian on 11/2/17.
 */

public class Query2CountCollator implements Collator<Map.Entry<String, Long>, Map<String, Long>> {

    private int n;
    private HazelcastInstance hz;
    public Query2CountCollator(int n, HazelcastInstance hz) {
        this.n = n;
        this.hz = hz;
    }

    @Override
    public Map<String, Long> collate( Iterable<Map.Entry<String, Long>> values ) {
        List<Map.Entry<String, Long>> list = new LinkedList<>((Collection<? extends Map.Entry<String, Long>>) values);
        list.sort((Map.Entry<String, Long> o1, Map.Entry<String, Long> o2)->o2.getValue().compareTo(o1.getValue()));

        int i = 0;
        Map<String, Long> aa = new HashMap<>();
        for (Map.Entry<String, Long> a :
                list) {
            if(i++ >= n)
                break;
            aa.put(a.getKey(), a.getValue());

        }
        return aa;
    }
}

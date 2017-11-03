package ar.edu.itba.pod.query5;

import com.hazelcast.mapreduce.Collator;

import java.util.*;

/**
 * Created by sebastian on 11/3/17.
 */
public class Query5Collator implements Collator<Map.Entry<String, Float>, Map<String, Float>> {
    @Override
    public Map<String, Float> collate(Iterable<Map.Entry<String, Float>> values) {
        List<Map.Entry<String, Float>> list = new LinkedList<>((Collection<? extends Map.Entry<String, Float>>) values);

        list.sort((Map.Entry<String, Float> o1, Map.Entry<String, Float> o2)->o2.getValue().compareTo(o1.getValue()));
        Map<String, Float> aa = new LinkedHashMap<>();

        for (Map.Entry<String, Float> a : list) {
            aa.put(a.getKey(), a.getValue());
        }
        return aa;
    }
}

package ar.edu.itba.pod.api;

import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class OrderStringIntMapCollator implements Collator<Map.Entry<String, Integer>, Map<String, Integer>>{
    @Override
    public Map<String, Integer> collate(Iterable<Map.Entry<String, Integer>> iterable) {
        List<Map.Entry<String, Integer>> list = StreamSupport.stream(iterable.spliterator(), false)
                .collect(Collectors.toList());
        //Collections.sort(list, Comparator.comparing(Map.Entry::getValue));
        Collections.sort(list, (o1, o2) -> o2.getValue().compareTo(o1.getValue()));
        Map<String, Integer> result = new LinkedHashMap<>();
        for (Map.Entry<String, Integer> entry: list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
}

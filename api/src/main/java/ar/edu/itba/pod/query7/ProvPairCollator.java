package ar.edu.itba.pod.query7;

import com.hazelcast.mapreduce.Collator;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ProvPairCollator implements Collator<Map.Entry<String, Integer>, Map<String, Integer>> {

    private int n;

    public ProvPairCollator(int n) {
        this.n = n;
    }
    @Override
    public Map<String, Integer> collate(Iterable<Map.Entry<String, Integer>> iterable) {
        List<Map.Entry<String, Integer>> list = StreamSupport.stream(iterable.spliterator(), false)
                .collect(Collectors.toList());
        Collections.sort(list, (o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        Map<String, Integer> result = new LinkedHashMap<>();

        for ( Map.Entry<String, Integer> entry : list) {
            if (entry.getValue() >= n) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }
}

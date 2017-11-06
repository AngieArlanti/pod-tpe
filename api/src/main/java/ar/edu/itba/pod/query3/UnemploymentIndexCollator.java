package ar.edu.itba.pod.query3;

import com.hazelcast.mapreduce.Collator;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class UnemploymentIndexCollator implements Collator<Map.Entry<String, Double>, Map<String, Double>>{
    @Override
    public Map<String, Double> collate(Iterable<Map.Entry<String, Double>> iterable) {
        List<Map.Entry<String, Double>> list = StreamSupport.stream(iterable.spliterator(), false)
                .collect(Collectors.toList());
        Collections.sort(list, (o1, o2) -> o2.getValue().compareTo(o1.getValue()));
        Map<String, Double> result = new LinkedHashMap<>();
        for (Map.Entry<String, Double> entry : list){
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
}

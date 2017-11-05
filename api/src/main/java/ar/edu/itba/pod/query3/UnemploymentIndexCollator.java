package ar.edu.itba.pod.query3;

import com.hazelcast.mapreduce.Collator;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class UnemploymentIndexCollator implements Collator<Map.Entry<String, BigDecimal>, Map<String, BigDecimal>> {
    @Override
    public Map<String, BigDecimal> collate(Iterable<Map.Entry<String, BigDecimal>> iterable) {
        List<Map.Entry<String, BigDecimal>> list = StreamSupport.stream(iterable.spliterator(), false)
                .collect(Collectors.toList());
        //Collections.sort(list, Comparator.comparing(Map.Entry::getValue));
        Collections.sort(list, (o1, o2) -> o2.getValue().compareTo(o1.getValue()));
        Map<String, BigDecimal> result = new LinkedHashMap<>();
        for (Map.Entry<String, BigDecimal> entry: list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
}

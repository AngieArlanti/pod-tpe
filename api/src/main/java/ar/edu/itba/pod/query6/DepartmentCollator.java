package ar.edu.itba.pod.query6;


import ar.edu.itba.pod.model.DepartmentNameOcurrenciesCount;
import com.hazelcast.mapreduce.Collator;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DepartmentCollator implements Collator<Map.Entry<String, DepartmentNameOcurrenciesCount>, Map<String, DepartmentNameOcurrenciesCount>> {

    private int n;

    public DepartmentCollator(int n) {
        this.n = n;
    }

    @Override
    public Map<String, DepartmentNameOcurrenciesCount> collate(Iterable<Map.Entry<String, DepartmentNameOcurrenciesCount>> iterable) {


        List<Map.Entry<String, DepartmentNameOcurrenciesCount>> list = StreamSupport.stream(iterable.spliterator(), false)
                .collect(Collectors.toList());
        Collections.sort(list, (o1, o2) -> o2.getValue().compareTo(o1.getValue()));

        Map<String, DepartmentNameOcurrenciesCount> result = new LinkedHashMap<>();

        for ( Map.Entry<String, DepartmentNameOcurrenciesCount> entry : list) {
            if (entry.getValue().getCount() >= n) {
                result.put(entry.getKey(), entry.getValue());
            }
        }
        return result;
    }
}

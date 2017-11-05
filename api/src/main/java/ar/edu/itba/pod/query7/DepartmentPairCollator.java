package ar.edu.itba.pod.query7;

import ar.edu.itba.pod.model.DepartmentNameOcurrenciesCount;
import ar.edu.itba.pod.model.DepartmentPairOcurrenciesCount;
import com.hazelcast.mapreduce.Collator;


import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;


public class DepartmentPairCollator implements Collator<Map.Entry<String, Map<String, String>>, Map<String, DepartmentPairOcurrenciesCount>> {

    private Map<String, DepartmentPairOcurrenciesCount> provincePairDepartmentPairOcurrenciesCountMap;
    private Map<String, Set<String>> pairDepartmentMap;
    private int n;

    public DepartmentPairCollator(int n) {
        this.n = n;
    }

    /**
     * En la salida del reducer tenemos un Map<Departamento,Map<parDeProvincias,Departamento>>
     * Lo que hacemos en este método es crear un Map<parDeProvincias,DepartmentPairOcurrenciesCount>
     *
     * @param iterable Mapas devueltos por el reducer
     * @return Map<parDeProvincias,DepartmentPairOcurrenciesCount>
     */
    @Override
    public Map<String, DepartmentPairOcurrenciesCount> collate(Iterable<Map.Entry<String, Map<String, String>>> iterable) {
        provincePairDepartmentPairOcurrenciesCountMap = new HashMap<>();
        pairDepartmentMap = new HashMap<>();

        for (Map.Entry<String, Map<String, String>> map : iterable) {
            Map<String, String> provincePairDepartmentMap = map.getValue();
            for (String provincePair : provincePairDepartmentMap.keySet()) {
                Set<String> departmentsForPair = new TreeSet<>();
                if (pairDepartmentMap.containsKey(provincePair)) {
                    departmentsForPair = pairDepartmentMap.get(provincePair);
                }
                departmentsForPair.add(map.getKey());
                pairDepartmentMap.put(provincePair, departmentsForPair);

                if (departmentsForPair.size() >= n) {
                    provincePairDepartmentPairOcurrenciesCountMap.put(provincePair, new DepartmentPairOcurrenciesCount(provincePair, departmentsForPair.size()));
                }
            }

        }
        return sortDescending(provincePairDepartmentPairOcurrenciesCountMap);
    }

    private Map<String, DepartmentPairOcurrenciesCount> sortDescending(Map<String, DepartmentPairOcurrenciesCount> map) {
        List<Map.Entry<String, DepartmentPairOcurrenciesCount>> list = new LinkedList<Map.Entry<String, DepartmentPairOcurrenciesCount>>(map.entrySet());
        Collections.sort(list, new Comparator<Map.Entry<String, DepartmentPairOcurrenciesCount>>() {
            @Override
            public int compare(Map.Entry<String, DepartmentPairOcurrenciesCount> o1, Map.Entry<String, DepartmentPairOcurrenciesCount> o2) {
                return o2.getValue().getCount().compareTo(o1.getValue().getCount());
            }
        });

        Map<String, DepartmentPairOcurrenciesCount> result = new LinkedHashMap<String, DepartmentPairOcurrenciesCount>();
        for (Map.Entry<String, DepartmentPairOcurrenciesCount> entry : list) {
            result.put(entry.getKey(), entry.getValue());
        }
        return result;
    }
}

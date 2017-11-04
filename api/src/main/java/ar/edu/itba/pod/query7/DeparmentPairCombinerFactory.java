package ar.edu.itba.pod.query7;

import ar.edu.itba.pod.model.ProvincePairDepartments;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.util.*;

public class DeparmentPairCombinerFactory implements CombinerFactory<String, String, ProvincePairDepartments> {

    //String keyIn: nombre departamento
    @Override
    public Combiner<String, ProvincePairDepartments> newCombiner(String key) {
        return new DepartmentPairCombiner(key);
    }

    private class DepartmentPairCombiner extends Combiner<String, ProvincePairDepartments> {

        private Set<String> provinceNames;
        private String departamentName;

        public DepartmentPairCombiner(String deptName) {
            this.departamentName = deptName;
        }

        @Override
        public void beginCombine() {
            super.beginCombine();
            provinceNames = new TreeSet<>();
        }

        //String value In : nombre provincia
        @Override
        public void combine(String provinceName) {
            provinceNames.add(provinceName);
        }

        // Set: ["departamento": lista de provincias"]
        @Override
        public ProvincePairDepartments finalizeChunk() {
            ProvincePairDepartments provincePairDepartments = new ProvincePairDepartments();

            List<String> provinces = new ArrayList<>();
            provinces.addAll(provinceNames);

            for (int i = 0; i< provinces.size(); i++) {
                for (int j = i+1 ; j < provinces.size(); j++) {
                    String provinceOne = provinces.get(i);
                    String provinceTwo = provinces.get(j);
                    String pair = null;
                    if (provinceOne.compareTo(provinceTwo) <= 0) {
                        pair = new StringBuilder().append(provinceOne).append(" + ").append(provinceTwo).toString();
                    } else if (provinceOne.compareTo(provinceTwo) > 0) {
                        pair = new StringBuilder().append(provinceTwo).append(" + ").append(provinceOne).toString();
                    }
                    provincePairDepartments.addPair(pair);
                }
            }
            return provincePairDepartments;
        }

        @Override
        public void reset() {
            super.reset();
            provinceNames = new TreeSet<>();
        }
    }
}

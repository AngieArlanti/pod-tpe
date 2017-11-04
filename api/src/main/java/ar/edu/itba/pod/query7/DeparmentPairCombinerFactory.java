package ar.edu.itba.pod.query7;

import ar.edu.itba.pod.model.DepartmentProvincePair;
import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.util.*;

public class DeparmentPairCombinerFactory implements CombinerFactory<String, String, DepartmentProvincePair> {

    //String keyIn: nombre departamento
    @Override
    public Combiner<String, DepartmentProvincePair> newCombiner(String key) {
        return new DepartmentPairCombiner(key);
    }

    private class DepartmentPairCombiner extends Combiner<String, DepartmentProvincePair> {

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
        public DepartmentProvincePair finalizeChunk() {
            DepartmentProvincePair departmentProvincePair = new DepartmentProvincePair();
            departmentProvincePair.setDepartmentName(departamentName);

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
                    departmentProvincePair.addPair(pair);
                }
            }
            return departmentProvincePair;
        }

        @Override
        public void reset() {
            super.reset();
            provinceNames = new TreeSet<>();
        }
    }
}

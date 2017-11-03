package ar.edu.itba.pod.query6;

import ar.edu.itba.pod.model.DepartmentNameOcurrenciesCount;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class DepartmentReducerFactory implements ReducerFactory<String, String, DepartmentNameOcurrenciesCount> {

    @Override
    public Reducer<String, DepartmentNameOcurrenciesCount> newReducer(String key) {
        return new DepartmentReducer(key);
    }

    private class DepartmentReducer extends Reducer<String, DepartmentNameOcurrenciesCount> {

        private Set<String> provinceSet;
        private String departmentName;

        public DepartmentReducer(String key) {
            this.departmentName = key;
        }

        @Override
        public void beginReduce() {
            provinceSet = new TreeSet<>();
        }

        @Override
        public void reduce(String provinceName) {
            provinceSet.add(provinceName);
        }

        @Override
        public DepartmentNameOcurrenciesCount finalizeReduce() {
            return new DepartmentNameOcurrenciesCount(departmentName, provinceSet.size());
        }
    }
}

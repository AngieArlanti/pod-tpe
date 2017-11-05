package ar.edu.itba.pod.query7;

import ar.edu.itba.pod.model.ProvincePairDepartments;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.HashMap;
import java.util.Map;

public class DepartmentPairReducerFactory implements ReducerFactory<String,ProvincePairDepartments,Map<String,String>

        > {
    @Override
    public Reducer<ProvincePairDepartments, Map<String,String>> newReducer(String departmentName) {
        return new DepartmentPairReducer(departmentName);
    }

    private class DepartmentPairReducer extends Reducer<ProvincePairDepartments,Map<String,String>>{
        Map<String,String> provincePairDepartmentMap;
        String dept;

        public DepartmentPairReducer(String dept) {
            this.dept = dept;
        }

        @Override
        public void beginReduce() {
            super.beginReduce();
            provincePairDepartmentMap = new HashMap<>();
        }

        @Override
        public void reduce(ProvincePairDepartments provincePairDepartments) {
            for(String pair: provincePairDepartments.getProvincePairSet()){
                provincePairDepartmentMap.put(pair,dept);
            }
        }

        @Override
        public Map<String,String> finalizeReduce() {
            return provincePairDepartmentMap;
        }
    }
}

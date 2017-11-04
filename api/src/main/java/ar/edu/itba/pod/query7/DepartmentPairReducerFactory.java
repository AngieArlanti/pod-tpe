package ar.edu.itba.pod.query7;

import ar.edu.itba.pod.model.DepartmentPairOcurrenciesCount;
import ar.edu.itba.pod.model.DepartmentProvincePair;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class DepartmentPairReducerFactory implements ReducerFactory<String,DepartmentProvincePair,DepartmentPairOcurrenciesCount> {
    @Override
    public Reducer<DepartmentProvincePair, DepartmentPairOcurrenciesCount> newReducer(String departmentName) {
        return null;
    }

    private class DepartmentPairReducer extends Reducer<DepartmentProvincePair,DepartmentPairOcurrenciesCount>{

        @Override
        public void beginReduce() {
            super.beginReduce();
        }

        @Override
        public void reduce(DepartmentProvincePair departmentProvincePair) {

        }

        @Override
        public DepartmentPairOcurrenciesCount finalizeReduce() {
            return null;
        }
    }
}

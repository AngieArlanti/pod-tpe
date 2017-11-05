package ar.edu.itba.pod.query7;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.ArrayList;
import java.util.List;

public class ProvPairReducerFactory implements ReducerFactory<String, String, Integer> {
    @Override
    public Reducer<String, Integer> newReducer(String s) {
        return new ProvPairReducer();
    }

    private class ProvPairReducer extends Reducer<String, Integer> {

        private int cantDepartments;

        @Override
        public void beginReduce() {
            cantDepartments = 0;
        }

        @Override
        public void reduce(String s) {
            cantDepartments ++;
        }

        @Override
        public Integer finalizeReduce() {
            return cantDepartments;
        }
    }
}

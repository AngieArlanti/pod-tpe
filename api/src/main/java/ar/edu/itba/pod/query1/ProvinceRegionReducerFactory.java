package ar.edu.itba.pod.query1;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class ProvinceRegionReducerFactory implements ReducerFactory<String, Integer, Integer>{

    @Override
    public Reducer<Integer, Integer> newReducer(String s) {
        return new ProvinceRegionReducer();
    }

    private class ProvinceRegionReducer extends Reducer<Integer, Integer> {

        private int sum;
        @Override
        public void beginReduce() {
            sum = 0;
        }

        @Override
        public void reduce(Integer integer) {
            sum += integer;
        }

        @Override
        public Integer finalizeReduce() {
            return sum;
        }
    }
}

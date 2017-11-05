package ar.edu.itba.pod.query4;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class HogarCountReducerFactory implements ReducerFactory<String, Long, Integer> {


    @Override
    public Reducer<Long, Integer> newReducer(String s) {
        return new HogarCountReducer();
    }

    private class HogarCountReducer extends Reducer<Long, Integer> {

        private int count;

        @Override
        public void beginReduce() {
            count=0;
        }

        @Override
        public void reduce(Long integer) {
            count++;
        }

        @Override
        public Integer finalizeReduce() {
            return count;
        }
    }
}

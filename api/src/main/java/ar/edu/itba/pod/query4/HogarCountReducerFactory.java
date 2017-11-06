package ar.edu.itba.pod.query4;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.ArrayList;
import java.util.List;

public class HogarCountReducerFactory implements ReducerFactory<String, Long, Integer> {


    @Override
    public Reducer<Long, Integer> newReducer(String s) {
        return new HogarCountReducer();
    }

    private class HogarCountReducer extends Reducer<Long, Integer> {

        private int count;
        private List<Long> homes;

        @Override
        public void beginReduce() {
            homes = new ArrayList<>();
            count=0;
        }

        @Override
        public void reduce(Long integer) {
            if (!homes.contains(integer)) {
                count++;
                homes.add(integer);
            }
        }

        @Override
        public Integer finalizeReduce() {
            return count;
        }
    }
}

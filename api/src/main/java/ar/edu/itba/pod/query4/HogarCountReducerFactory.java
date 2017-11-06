package ar.edu.itba.pod.query4;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.Set;
import java.util.TreeSet;

public class HogarCountReducerFactory implements ReducerFactory<String, Long, Integer> {


    @Override
    public Reducer<Long, Integer> newReducer(String s) {
        return new HogarCountReducer();
    }

    private class HogarCountReducer extends Reducer<Long, Integer> {

        private Set<Long> hogares;

        @Override
        public void beginReduce() {
            hogares = new TreeSet<>();
        }

        @Override
        public void reduce(Long hogarId) {
            hogares.add(hogarId);
        }

        @Override
        public Integer finalizeReduce() {
            return hogares.size();
        }
    }
}

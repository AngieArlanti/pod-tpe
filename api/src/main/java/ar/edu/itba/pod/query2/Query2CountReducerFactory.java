package ar.edu.itba.pod.query2;

import com.hazelcast.mapreduce.Reducer;

/**
 * Created by sebastian on 10/31/17.
 */
public class Query2CountReducerFactory implements com.hazelcast.mapreduce.ReducerFactory<String, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer( String key ) {
        return new Query2CountReducerFactory.CitizentCountReducer();
    }
    private class CitizentCountReducer extends Reducer<Long, Long> {
        private volatile long sum;
        @Override
        public void beginReduce () {
            sum = 0;
        }
        @Override
        public void reduce( Long value ) {
            sum += value.longValue();
        }
        @Override
        public Long finalizeReduce() {
            return sum;
        }
    }
}

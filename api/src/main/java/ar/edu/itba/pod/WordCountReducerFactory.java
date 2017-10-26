package ar.edu.itba.pod;

import com.hazelcast.mapreduce.Reducer;

/**
 * Created by marlanti on 10/9/17.
 */
public class WordCountReducerFactory implements com.hazelcast.mapreduce.ReducerFactory<String, Long, Long> {

    @Override
    public Reducer<Long, Long> newReducer( String key ) {
        return new WordCountReducer();
    }
    private class WordCountReducer extends Reducer<Long, Long> {
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



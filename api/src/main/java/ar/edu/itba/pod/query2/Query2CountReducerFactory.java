package ar.edu.itba.pod.query2;

import com.hazelcast.mapreduce.Reducer;

/**
 * Created by sebastian on 10/31/17.
 */
public class Query2CountReducerFactory implements com.hazelcast.mapreduce.ReducerFactory<String, Long, Long> {
    @Override
    public Reducer<Long, Long> newReducer(String key) {
        return null;
    }
}

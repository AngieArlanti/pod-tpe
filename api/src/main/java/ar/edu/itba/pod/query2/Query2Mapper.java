package ar.edu.itba.pod.query2;

import com.hazelcast.mapreduce.Context;

import java.util.Collection;
import java.util.StringTokenizer;

/**
 * Created by sebastian on 10/31/17.
 */
public class Query2Mapper implements com.hazelcast.mapreduce.Mapper<String, Collection<Long>, String, Long> {

    // TODO make this more efficient
    @Override
    public void map(String s, Collection<Long> longs, Context<String, Long> context) {
        for (Long l : longs) {
            context.emit(s, l);
        }
    }
}

package ar.edu.itba.pod.query5;

import ar.edu.itba.pod.Cit;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

/**
 * Created by sebastian on 11/3/17.
 */
public class Query5Mapper implements Mapper<String, Cit, String, Long> {
    @Override
    public void map(String s, Cit c, Context<String, Long> context) {
        context.emit(c.getRegion().toString(), c.getHomeId());
    }
}

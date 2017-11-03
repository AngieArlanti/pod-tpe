package ar.edu.itba.pod.query5;

import ar.edu.itba.pod.Cit;
import ar.edu.itba.pod.model.Data;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

/**
 * Created by sebastian on 11/3/17.
 */
public class Query5Mapper implements Mapper<String, Data, String, Long> {
    @Override
    public void map(String s, Data c, Context<String, Long> context) {
        context.emit(c.getRegion(), c.getHomeId());
    }
}

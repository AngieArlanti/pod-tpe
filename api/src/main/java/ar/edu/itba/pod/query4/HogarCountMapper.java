package ar.edu.itba.pod.query4;

import ar.edu.itba.pod.model.Data;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class HogarCountMapper implements Mapper<String, Data, String, Long> {

    @Override
    public void map(String s, Data input, Context<String, Long> context) {
        context.emit(input.getRegion(), input.getHomeId());

    }
}

package ar.edu.itba.pod.query1;

import ar.edu.itba.pod.model.Data;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class ProvinceRegionMapper implements Mapper<String, Data, String, Integer>{

    @Override
    public void map(String keyIn, Data valueIn, Context<String, Integer> context) {
        context.emit(valueIn.getRegion(), 1);
    }
}

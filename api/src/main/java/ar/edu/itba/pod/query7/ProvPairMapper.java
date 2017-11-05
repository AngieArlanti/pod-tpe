package ar.edu.itba.pod.query7;

import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

import java.util.Map;

public class ProvPairMapper implements Mapper<String, Map<String, String>, String, String> {
    @Override
    public void map(String keyIn, Map<String, String> valueIn, Context<String, String> context) {
        for (String key : valueIn.keySet()){
            context.emit(key, valueIn.get(key));
        }
    }
}

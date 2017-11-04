package ar.edu.itba.pod.query7;

import ar.edu.itba.pod.model.Data;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class DepartmentMapper implements Mapper<String, Data, String, String> {

    @Override
    public void map(String s, Data data, Context<String, String> context) {
        context.emit(data.getDepartmentName(), data.getProvinceName());
    }
}

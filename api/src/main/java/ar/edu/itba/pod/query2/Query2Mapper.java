package ar.edu.itba.pod.query2;

import ar.edu.itba.pod.model.Data;
import com.hazelcast.mapreduce.Context;

/**
 * Created by sebastian on 10/31/17.
 */
public class Query2Mapper implements com.hazelcast.mapreduce.Mapper<String, Data, String, Long> {

    @Override
    public void map(String s, Data d, Context<String, Long> context) {
        context.emit(d.getDepartmentName(),1L);
    }
}

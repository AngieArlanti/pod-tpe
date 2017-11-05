package ar.edu.itba.pod.query3;

import ar.edu.itba.pod.model.ActivityCondition;
import ar.edu.itba.pod.model.Data;
import com.hazelcast.mapreduce.Context;
import com.hazelcast.mapreduce.Mapper;

public class UnemploymentIndexMapper implements Mapper<String, Data, String, ActivityCondition> {
    @Override
    public void map(String keyIn, Data valueIn, Context<String, ActivityCondition> context) {
        if(valueIn.getActivityCondition().equals(ActivityCondition.NOTBUSY) ||
                valueIn.getActivityCondition().equals(ActivityCondition.BUSY)){
            System.out.println("region =  " + valueIn.getRegion() +" actividad = " +valueIn.getActivityCondition());
            context.emit(valueIn.getRegion(), valueIn.getActivityCondition());
        }
    }
}

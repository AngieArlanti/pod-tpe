package ar.edu.itba.pod.query3;

import ar.edu.itba.pod.model.ActivityCondition;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

public class UnemploymentIndexReducerFactory implements ReducerFactory<String, ActivityCondition, Double> {
    @Override
    public Reducer<ActivityCondition, Double> newReducer(String s) {
        return new UnemploymentIndexReducer();
    }

    private class UnemploymentIndexReducer extends Reducer<ActivityCondition, Double> {

        private int desocupados;
        private int ocupados;

        @Override
        public void beginReduce() {
            desocupados = 0;
            ocupados = 0;
        }

        @Override
        public void reduce(ActivityCondition condition) {
            if(condition.equals(ActivityCondition.NOTBUSY)){
                desocupados++;
            } else
                ocupados++;

        }

        @Override
        public Double finalizeReduce() {
            return (double) desocupados/(ocupados+desocupados);
        }
    }
}

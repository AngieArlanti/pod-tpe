package ar.edu.itba.pod.query3;

import ar.edu.itba.pod.model.ActivityCondition;
import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.math.BigDecimal;

public class UnemploymentIndexReducerFactory implements ReducerFactory<String, ActivityCondition, BigDecimal> {
    @Override
    public Reducer<ActivityCondition, BigDecimal> newReducer(String s) {
        return new UnemploymentIndexReducer();
    }

    private class UnemploymentIndexReducer extends Reducer<ActivityCondition, BigDecimal> {

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
        public BigDecimal finalizeReduce() {
            return new BigDecimal((float)desocupados/(float)(ocupados+desocupados))
                    .setScale(2, BigDecimal.ROUND_HALF_UP);
        }
    }
}

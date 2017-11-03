package ar.edu.itba.pod.query5;

import com.hazelcast.mapreduce.Reducer;
import com.hazelcast.mapreduce.ReducerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Created by sebastian on 11/3/17.
 */
public class Query5ReducerFactory implements ReducerFactory<String, Collection<Long>, Float> {
    @Override
    public Reducer<Collection<Long>, Float> newReducer(String s) {
        RegionAverager ra =  new RegionAverager();
        return ra;
    }
    private class RegionAverager extends Reducer<Collection<Long>, Float> {
        private volatile Map<Long, Integer> map;
        @Override
        public void beginReduce () {
            map = new HashMap<>();
        }
        @Override
        public void reduce( Collection<Long> values ) {
            for (Long v :
                    values) {
                if (!map.containsKey(v))
                    map.put(v,0);
                map.put(v,map.get(v)+1);
            }
        }
        @Override
        public Float finalizeReduce() {
            Float ans = new Float(map.values().stream().mapToInt(Integer::intValue).sum())/map.size();
            ans = (float)((int)(ans * 100))/100;
            return ans;
        }
    }
}

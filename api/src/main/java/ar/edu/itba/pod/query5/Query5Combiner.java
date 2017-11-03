package ar.edu.itba.pod.query5;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

import java.util.ArrayList;
import java.util.Collection;

/**
 * Created by sebastian on 11/3/17.
 */
public class Query5Combiner implements CombinerFactory<String, Long, Collection<Long>> {

    @Override
    public Combiner<Long, Collection<Long>> newCombiner (String key) {
        return new personsPerHomeCombiner();
    }

    class personsPerHomeCombiner extends Combiner<Long, Collection<Long>> {
        private ArrayList<Long> aux = new ArrayList<>();
        @Override
        public void combine( Long value ) {
            aux.add(value);
        }
        @Override
        public Collection<Long> finalizeChunk() {
            return aux;
        }
        @Override
        public void reset() {
            aux = new ArrayList<>();
        }
    }

}

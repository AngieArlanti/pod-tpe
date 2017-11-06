package ar.edu.itba.pod.query2;

import com.hazelcast.mapreduce.Combiner;
import com.hazelcast.mapreduce.CombinerFactory;

/**
 * Created by sebastian on 11/6/17.
 */
public class Query2Combiner implements CombinerFactory<String, Long, Long> {

    @Override
    public Combiner<Long, Long> newCombiner(String s) {
        return new CitizensCombiner();
    }

    class CitizensCombiner extends Combiner<Long, Long> {

        private long sum = 0;

        @Override
        public void combine(Long aLong) {
            sum++;
        }

        @Override
        public Long finalizeChunk() {
            return sum;
        }

        @Override
        public void reset() {
            this.sum = 0;
        }
    }
}

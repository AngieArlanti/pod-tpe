package ar.edu.itba.pod.query2;

import ar.edu.itba.pod.model.Data;
import com.hazelcast.mapreduce.KeyPredicate;

/**
 * Created by sebastian on 11/3/17.
 */
public class Query2Predicate implements KeyPredicate<Data> {

    private String provinceName;
    public Query2Predicate(String provinceName) {
        this.provinceName = provinceName;
    }

    @Override
    public boolean evaluate(Data d) {
        return d != null && d.getProvinceName().toLowerCase().equals(this.provinceName.toLowerCase());
    }
}

package ar.edu.itba.pod.example;

import com.hazelcast.mapreduce.Context;

import java.util.StringTokenizer;

/**
 * Created by marlanti on 10/9/17.
 */
public class TokenizerMapper implements com.hazelcast.mapreduce.Mapper<String, String, String, Long> {
    private static final Long ONE = 1L;

    @Override
    public void map(String key, String line, Context<String, Long> context) {
        StringTokenizer tokenizer = new StringTokenizer( line.toLowerCase() );
        while ( tokenizer.hasMoreTokens() ) {
            context.emit( tokenizer.nextToken(), ONE );
        }
    }
}

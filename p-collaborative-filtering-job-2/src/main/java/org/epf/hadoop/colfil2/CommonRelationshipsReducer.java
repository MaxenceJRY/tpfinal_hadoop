package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

public class CommonRelationshipsReducer extends Reducer<UserPair, IntWritable, UserPair, IntWritable> {

    @Override
    protected void reduce(UserPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        boolean isValid = true;

        for (IntWritable value : values) {
            if (value.get() == 0) {
                isValid = false;
                break;
            }
            count += value.get();
        }

        if (isValid && count > 0) {
            context.write(key, new IntWritable(count));
        }
    }
}

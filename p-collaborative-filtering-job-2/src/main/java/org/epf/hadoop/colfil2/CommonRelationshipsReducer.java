package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CommonRelationshipsReducer extends Reducer<UserPair, IntWritable, UserPair, IntWritable> {
    protected void reduce(UserPair key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int comp = 0;
        for (IntWritable value : values) {
            if (value.get() == 1) {
                comp++;
            }else {
                comp = 0;
                break;
            }
        }

        if (comp > 0) {
            context.write(key, new IntWritable(comp));
        }
    }

}
package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class CommonRelationshipsMapper extends Mapper<LongWritable, Text, UserPair, IntWritable> {

    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\t");
        String firstUser = tokens[0];
        String[] secondUserAndRelationship = tokens[1].split(",");
        List<String> relationships = Arrays.asList(secondUserAndRelationship);

        for (int i = 0; i < relationships.size(); i++) {
            for (int j = i + 1; j < relationships.size(); j++) {
                UserPair userPair = new UserPair(relationships.get(i), relationships.get(j));
                context.write(userPair, new IntWritable(1));
            }
        }

        for (String relationship : relationships) {
            UserPair userPair = new UserPair(firstUser, relationship);
            context.write(userPair, new IntWritable(0));
        }
    }

}


package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CommonRelationshipsMapper extends Mapper<LongWritable, Text, UserPair, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\t");

        if (tokens.length < 2) {
            return;
        }

        String firstUser = tokens[0];
        String[] secondUserAndRelationship = tokens[1].split(",");

        Set<String> relationshipsSet = new HashSet<>();
        for (String relationship : secondUserAndRelationship) {
            relationshipsSet.add(relationship.trim());
        }

        String[] relationships = relationshipsSet.toArray(new String[0]);
        for (int i = 0; i < relationships.length; i++) {
            for (int j = i + 1; j < relationships.length; j++) {
                UserPair userPair = new UserPair(relationships[i], relationships[j]);
                context.write(userPair, new IntWritable(1));
            }
        }

        for (String relationship : relationshipsSet) {
            UserPair userPair = new UserPair(firstUser, relationship);
            context.write(userPair, new IntWritable(0));
        }
    }
}

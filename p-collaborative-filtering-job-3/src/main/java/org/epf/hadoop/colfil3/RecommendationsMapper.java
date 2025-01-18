package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class RecommendationsMapper extends Mapper<Object, Text, Text, Text> {

    private Text userKey = new Text();
    private Text recommendation = new Text();

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] tokens = line.split("\t");
        if (tokens.length < 2) return;

        String[] users = tokens[0].split("\\.");
        String user1 = users[0];
        String user2 = users[1];
        String relationCount = tokens[1];

        userKey.set(user1);
        recommendation.set(user2 + ": " + relationCount);
        context.write(userKey, recommendation);

        userKey.set(user2);
        recommendation.set(user1 + ": " + relationCount);
        context.write(userKey, recommendation);
    }
}

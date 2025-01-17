package org.epf.hadoop.colfil3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

import java.util.*;

public class RecommendationsReducer extends Reducer<Text, Text, Text, Text> {

    private static final int MAX_RECOMMENDATIONS = 5;

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        List<String> recommendationsList = new ArrayList<>();

        for (Text value : values) {
            recommendationsList.add(value.toString());
        }

        recommendationsList.sort((a, b) -> {
            String[] tokensA = a.split("<=>");
            String[] tokensB = b.split("<=>");
            return Integer.parseInt(tokensB[1]) - Integer.parseInt(tokensA[1]);
        });

        List<String> topRecommendations;
        topRecommendations = recommendationsList.subList(0, Math.min(MAX_RECOMMENDATIONS, recommendationsList.size()));

        String topRecommendationsString = String.join(", ", topRecommendations);
        // Output the user and their top recommendations
        context.write(key, new Text(topRecommendationsString));
    }
}

package org.epf.hadoop.colfil2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ColFilJob2 {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: CommonRelationsJob <input path> <output path>");
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Common Relations Job");

        job.setJarByClass(ColFilJob2.class);

        job.setMapperClass(CommonRelationshipsMapper.class);
        job.setReducerClass(CommonRelationshipsReducer.class);
        //job.setPartitionerClass(UserPairPartitioner.class);

        job.setOutputKeyClass(UserPair.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setNumReduceTasks(2);

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

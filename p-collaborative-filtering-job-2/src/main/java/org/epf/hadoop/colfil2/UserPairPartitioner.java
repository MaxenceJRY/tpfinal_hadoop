package org.epf.hadoop.colfil2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class UserPairPartitioner extends Partitioner<UserPair, Text> {
    @Override
    public int getPartition(UserPair key, Text value, int numPartitions) {
        String firstUser = key.getFirstUser();
        int partition;

        if (firstUser.charAt(0) >= 'A' && firstUser.charAt(0) <= 'M') {
            partition = 0;
        } else {
            partition = 1;
        }

        System.out.println("Partitioner Key: " + key + ", Partition: " + partition);
        return partition;
    }
}

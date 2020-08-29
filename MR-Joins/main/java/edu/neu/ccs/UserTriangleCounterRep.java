package edu.neu.ccs;

import java.io.File;
import java.io.FileReader;
import java.net.URI;
import java.util.*;
import java.io.BufferedReader;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class UserTriangleCounterRep extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(UserTriangleCounterRep.class);
    private static final String COMMA_DELIMITER = ",";
    private enum EnumCounter{TRIANGLE_COUNT};
    private static final Integer MAX_FILTER = 10000;

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Map Side Join");
        job.setJarByClass(UserTriangleCounterRep.class);

        job.setMapperClass(TriangleEdgeMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // S <- Set as Job Input
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // T <- Broadcast to distributed file cache
        job.addCacheFile(new URI(args[0] + "/edges.csv"));

        boolean isJobComplete = job.waitForCompletion(true);

        Counters counter = job.getCounters();
        Counter triangleCount = counter.findCounter(EnumCounter.TRIANGLE_COUNT);
        long countVal = triangleCount.getValue() / 3;
        logger.info("\n" + triangleCount.getDisplayName() + " = " + countVal);

        return isJobComplete ? 0 : 1;
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Two arguments required:\n<input-dir> <output-dir>");
        }
        try {
            ToolRunner.run(new UserTriangleCounterRep(), args);
        }
        catch (Exception e) {
            logger.error("", e);
        }
    }

    public static class TriangleEdgeMapper extends Mapper<Object, Text, Text, Text> {

        private Map<String, Set<String>> userConnection = new HashMap<>();

        @Override
        public void setup(final Context context)
                throws IOException, InterruptedException {

            BufferedReader rdr = null;

            try {
                URI[] files = context.getCacheFiles();
                logger.info("Files in cache: " + Arrays.toString(files));

                if (files == null || files.length == 0) {
                    throw new RuntimeException("File not set in cache");
                }

                for (URI f : files) {

                    Path fp = new Path(f);
                    File filename = new File(fp.toString());

                    rdr = new BufferedReader(new FileReader(filename));

                    String row;
                    while ((row = rdr.readLine()) != null) {
                        String[] rowValues = row.split(COMMA_DELIMITER);
                        if (rowValues.length != 2) {
                            return;
                        }
                        String follower = rowValues[0];
                        String user = rowValues[1];

                        if (Integer.parseInt(follower) < MAX_FILTER && Integer.parseInt(user) < MAX_FILTER) {
                            userConnection.computeIfAbsent(user, u -> new HashSet<>()).add(follower);
                        }
                    }
                }
            }
            catch (Exception e) {
                logger.info("Error occurred while creating cache " + e.getMessage());
            }
            finally {
                if (rdr != null) {
                    rdr.close();
                }
            }
        }

        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {

            String[] rowValues = value.toString().split(COMMA_DELIMITER);

            if (rowValues.length != 2) {
                return;
            }
            String follower = rowValues[0];
            String user = rowValues[1];
            // if the follower is in userConnection, check if the user is connected
            // with any of the followers connections
            if (Integer.parseInt(follower) < MAX_FILTER && Integer.parseInt(user) < MAX_FILTER) {
                Set<String> connections = userConnection.get(follower);
                if (CollectionUtils.isNotEmpty(connections)) {
                    for (String c : connections) {
                        Set<String> connectionsOfC = userConnection.get(c);
                        if (CollectionUtils.isNotEmpty(connectionsOfC) && connectionsOfC.contains(user)) {
                            context.getCounter(EnumCounter.TRIANGLE_COUNT).increment(1);

                        }
                    }
                }

            }
        }
    }
}

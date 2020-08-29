package edu.neu.ccs;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org .apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class UserTriangleCounterRS extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(UserTriangleCounterRS.class);
    private enum EnumCounter{PATH2, TRIANGLE_COUNT};
    private static final String COMMA_DELIMITER = ",";
    private static final Integer MAX_FILTER = 50000;

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job1 = Job.getInstance(conf, "Reduce Side Join");
        job1.setJarByClass(UserTriangleCounterRS.class);

        job1.setMapperClass(MapperStep1.class);
        job1.setReducerClass(ReducerStep1.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));

        job1.waitForCompletion(true );

        final Job job2 = Job.getInstance(conf, "Reduce Side Join Social Triangle Counter");
        job2.setJarByClass(UserTriangleCounterRS.class);

        job2.setMapperClass(MapperStep2.class);
        job2.setReducerClass(ReducerStep2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job2, new Path(args[1]), TextInputFormat.class, MapperStep2.class);
        MultipleInputs.addInputPath(job2, new Path(args[0]), TextInputFormat.class, MapperStep2.class);
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));

        boolean isJob2Complete = job2.waitForCompletion(true);

        Counters counter = job2.getCounters();
        Counter triangleCount = counter.findCounter(EnumCounter.TRIANGLE_COUNT);
        long countVal = triangleCount.getValue() / 3;
        logger.info("\n" + triangleCount.getDisplayName() + " = " + countVal);

        return isJob2Complete ? 0 : 1;
    }

    public static void main(String[] args) {
        if (args.length != 3) {
            throw new IllegalArgumentException("Three arguments required:\n<input-dir> <output-step1-dir> <output-dir>");
        }
        try {
            ToolRunner.run(new UserTriangleCounterRS(), args);
        }
        catch (final Exception e) {
            logger.error("", e);
        }
    }

    public static class MapperStep1 extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {

            String follower;
            String user;

            String[] rowValue = value.toString().split(COMMA_DELIMITER);

            if (rowValue.length != 2) {
                return;
            }
            else {
                follower = rowValue[0];
                user = rowValue[1];
            }

            if (Integer.parseInt(follower) < MAX_FILTER && Integer.parseInt(user) < MAX_FILTER) {

                outkey.set(user);
                outvalue.set("F" + follower);
                context.write(outkey, outvalue);

                outkey.set(follower);
                outvalue.set("T" + user);
                context.write(outkey, outvalue);
            }
        }
    }

    public static class ReducerStep1 extends Reducer<Text, Text, Text, Text> {

        private final List<Text> fromList = new ArrayList<>();
        private final List<Text> toList = new ArrayList<>();

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void reduce(final Text key, final Iterable<Text> values, final Context context)
                throws IOException, InterruptedException {

            fromList.clear();
            toList.clear();

            separateUsersTF(values, fromList, toList);
            join(context);
        }
        // implement inner join logic
        public void join(Context context) throws IOException, InterruptedException {

            if (!fromList.isEmpty() && !toList.isEmpty()) {
                for (Text fromUser : fromList) {
                    for (Text toUser : toList) {
                        outkey.set(fromUser);
                        // from reducer flag
                        outvalue.set("R" + toUser);
                        context.write(outkey, outvalue);
                        context.getCounter(EnumCounter.PATH2).increment(1);
                    }
                }
            }
        }
    }

    public static class MapperStep2 extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {

            String follower;
            String user;

            String[] rowValues = value.toString().split(COMMA_DELIMITER);

            if (rowValues.length != 2) {
                return;
            }
            else {
                follower = rowValues[0];
                user = rowValues[1];
            }

            if (user.charAt(0) == 'R') {
                outkey.set(follower);
                outvalue.set("T" + user.substring(1));
            }
            else {
                outkey.set(user);
                outvalue.set("F" + follower);
            }
            context.write(outkey, outvalue);
        }
    }

    public static class ReducerStep2 extends Reducer<Text, Text, Text, Text> {

        private List<Text> fromList = new ArrayList<>();
        private List<Text> toList = new ArrayList<>();

        public void reduce(final Text key, final Iterable<Text> values, final Context context)
                throws IOException, InterruptedException {

            fromList.clear();
            toList.clear();

            separateUsersTF(values, fromList, toList);

            if (!fromList.isEmpty() && !toList.isEmpty()) {
                fromList.sort(Comparator.comparing(Text::toString));
            }
            for (Text searchVal : toList) {
                if (Collections.binarySearch(fromList, searchVal) > -1) {
                    context.getCounter(EnumCounter.TRIANGLE_COUNT).increment(1);
                }
            }

        }
    }

    /**
     * @param values list of values to separate into fromList and toList
     * @param fromList list of followers
     * @param toList list of users being followed
     */
    private static void separateUsersTF(Iterable<Text> values, List<Text> fromList, List<Text> toList) {

        for (Text value : values) {
            if (value.charAt(0) == 'F') {
                fromList.add(new Text(value.toString().substring(1)));
            }
            else if (value.charAt(0) == 'T') {
                toList.add(new Text(value.toString().substring(1)));
            }
        }
    }
}

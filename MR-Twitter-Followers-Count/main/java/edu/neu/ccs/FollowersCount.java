package edu.neu.ccs;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Hadoop MapReduce implementation for counting number of followers for each user on Twitter
 */
public class FollowersCount extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(FollowersCount.class);

    /**
     * Mapper Implementation
     */
    public static class FollowersMapper extends Mapper<Object, Text, Text, IntWritable> {

        private static final IntWritable one = new IntWritable(1);
        private final Text user = new Text();

        /**
         * @param key                       input key (e.g. document name or id)
         * @param value                     value corresponding to the document
         * @param context                   program context
         * @throws IOException              program error
         * @throws InterruptedException     program error
         */
        @Override
        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            final StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            while (itr.hasMoreTokens()) {
                // skip the follower
                itr.nextToken();
                // get the user being followed
                user.set(itr.nextToken());
                context.write(user, one);
            }
        }
    }

    /**
     * Reducer implementation
     */
    public static class FollowersSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable totalFollowersCount = new IntWritable();

        /**
         * @param key                       input key (e.g. userID)
         * @param values                    values corresponding to the key, list of followers count
         * @param context                   program context
         * @throws IOException              program error
         * @throws InterruptedException     program error
         */
        @Override
        public void reduce(final Text key, final Iterable<IntWritable> values, final Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (final IntWritable val : values) {
                count += val.get();
            }
            totalFollowersCount.set(count);
            context.write(key, totalFollowersCount);
        }

    }

    /**
     * @param args program runner arguments
     * @return job waiting status
     * @throws Exception error during execution
     */
    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Twitter Followers Count");
        job.setJarByClass(FollowersCount.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", ",");

        job.setMapperClass(FollowersMapper.class);
        job.setCombinerClass(FollowersSumReducer.class);
        job.setReducerClass(FollowersSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true ) ? 0 : 1;
    }

    /**
     * @param args program input values
     */
    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }
        try {
            ToolRunner.run(new FollowersCount(), args);
        }
        catch (final Exception e){
            logger.error("", e);
        }

    }
}
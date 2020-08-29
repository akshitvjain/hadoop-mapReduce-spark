package edu.neu.ccs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class CountPath2 extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(CountPath2.class);
    private static final String COMMA_DELIMITER = ",";
    private enum COUNT_ENUM{PATH2_COUNT};

    @Override
    public int run(String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Count In Out");
        job.setJarByClass(CountPath2.class);

        job.setMapperClass(MapperIncomingOutgoing.class);
        job.setReducerClass(ReducerCount.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean isJobComplete = job.waitForCompletion(true);

        Counters counter = job.getCounters();
        Counter path2 = counter.findCounter(COUNT_ENUM.PATH2_COUNT);
        logger.info("\n" + path2.getDisplayName() + " = " + path2.getValue());

        return isJobComplete ? 0 : 1;
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Two arguments required:\n<input-dir> <output-dir>");
        }
        try {
            ToolRunner.run(new CountPath2(), args);
        }
        catch (final Exception e) {
            logger.error("", e);
        }
    }

    public static class MapperIncomingOutgoing extends Mapper<Object, Text, Text, Text> {

        private Text outkey = new Text();
        private Text outvalue = new Text();

        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {

            String[] rowValues = value.toString().split(COMMA_DELIMITER);

            if (rowValues.length != 2) {
                return;
            }

            String follower = rowValues[0];
            String followed = rowValues[1];

            outkey.set(follower);
            outvalue.set("O" + followed);
            context.write(outkey, outvalue);

            outkey.set(followed);
            outvalue.set("I" + follower);
            context.write(outkey, outvalue);
        }
    }

    public static class ReducerCount extends Reducer<Text, Text, Text, Text> {

        private List<Integer> outList = new ArrayList<>();
        private List<Integer> inList = new ArrayList<>();

        public void reduce(final Text key, final Iterable<Text> values, final Context context)
                throws IOException, InterruptedException {

            outList.clear();
            inList.clear();

            for (Text value : values) {
                final Integer count = new Integer(value.toString().substring(1));
                if (value.charAt(0) == 'O') {
                    outList.add(count);
                }
                else if (value.charAt(0) == 'I') {
                    inList.add(count);
                }
            }
            int outSum = outList.size();
            int inSum = inList.size();
            Text userInOutCount = new Text(String.valueOf(outSum * inSum));
            context.getCounter(COUNT_ENUM.PATH2_COUNT).increment(outSum * inSum);
            context.write(key, userInOutCount);
        }
    }
}

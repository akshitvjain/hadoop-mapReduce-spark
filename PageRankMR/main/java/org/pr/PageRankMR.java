package org.pr;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;

public class PageRankMR extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(PageRankMR.class);
    private enum EnumCounter{DELTA, NUM_NODES};

    public static class MapperPR extends Mapper<Object, Text, Object, Text> {

        public void map(final Object key, final Text value, final Context context)
                throws IOException, InterruptedException {
            context.getCounter(EnumCounter.NUM_NODES).increment(1);
            Node node = new Node(value.toString());
            context.write(new Text(node.getId()), value);
            double pr = node.getPageRank();
            context.write(new Text(node.getNeighbor()), new Text(Double.toString(pr)));
        }
    }

    public static class ReducerPR extends Reducer<Text, Text, Text, Text> {

        private double delta = 0.0;
        private long numNodes = 0;

        public void setup(final Context context) {
            delta = context.getConfiguration().getDouble("delta", 0.0);
            numNodes = context.getConfiguration().getLong("numNodes", Integer.MAX_VALUE);
        }
        public void reduce(final Text key, final Iterable<Text> values, final Context context)
            throws IOException, InterruptedException {
            Double pr_ = 0.0;
            Node updateNode = null;
            for (Text val : values) {
                Node n = new Node(val.toString());
                if (n.isNode()) {
                    updateNode = n;
                }
                else {
                    if (key.equals(new Text(Integer.toString(0)))) {
                        long danglingCount = (long) (n.getPageRank() * 10000);
                        context.getCounter(EnumCounter.DELTA).increment(danglingCount);
                    }
                    else {
                        pr_ += n.getPageRank();
                    }
                }
            }
            double updatedPR = pr_ + (delta / numNodes);
            if (updateNode != null) {
                updateNode.setPageRank(updatedPR);
                context.write(new Text(updateNode.getId()),
                        new Text(updateNode.getNeighbor() + "\t" + updateNode.getPageRank().toString()));
            }
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        boolean isJobComplete = true;
        String input, output;
        int iterations = 10;
        for (int i = 0; i < iterations; i++) {
            Configuration conf = getConf();
            Job job = Job.getInstance(conf, "org.pr.PageRankMR");
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(MapperPR.class);
            job.setReducerClass(ReducerPR.class);
            job.setInputFormatClass(NLineInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            job.setJarByClass(PageRankMR.class);
            if (i == 0) {
                input = args[0];
            }
            else {
                input = args[1] + i;
            }
            output = args[1] + (i + 1);
            NLineInputFormat.addInputPath(job, new Path(input));
            job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", 30000);
            FileOutputFormat.setOutputPath(job, new Path(output));

            isJobComplete = job.waitForCompletion(true);

            Counters counter = job.getCounters();
            Counter delta = counter.findCounter(EnumCounter.DELTA);
            double lostPR = (double) delta.getValue() / 10000;
            conf.setDouble("delta", lostPR);

            long numNodes  = counter.findCounter(EnumCounter.NUM_NODES).getValue();
            conf.setLong("numNodes", numNodes);

            logger.info("PageRank Loss, Iteration: " + i + " " + lostPR);
        }
        return isJobComplete ? 0 : 1;
    }

    public static void main(final String[] args) {
        if (args.length != 2) {
            throw new Error("Two arguments required:\n<input-dir> <output-dir>");
        }
        try {
            ToolRunner.run(new PageRankMR(), args);
        }
        catch (final Exception e) {
            logger.error("", e);
        }
    }
}
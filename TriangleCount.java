import java.io.IOException;
import java.util.*;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

public class TriangleCount {
    public static class PreprocessorMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, LongWritable, LongWritable>.Context context)
                throws IOException, InterruptedException {
            String[] pair = value.toString().split("\\s+");
            // check if edge is valid (having 2 id)
            if (pair.length > 1) {
                long u = Long.parseLong(pair[0]);
                long v = Long.parseLong(pair[1]);
                // write the id pair smaller in left (becomes undirected)
                if (u < v) {
                    context.write(new LongWritable(u), new LongWritable(v));
                } else {
                    context.write(new LongWritable(v), new LongWritable(u));
                }
            }
        }

    }

    public static class PreprocessorReducer extends Reducer<LongWritable, LongWritable, Text, Text> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values,
                Reducer<LongWritable, LongWritable, Text, Text>.Context context)
                throws IOException, InterruptedException {
            ArrayList<Long> valuesList = new ArrayList<Long>();
            for (LongWritable val : values) {
                valuesList.add(val.get());
                // emit and imply connected by "$"
                context.write(new Text(key.toString() + ',' + val.toString()), new Text("$"));
            }

            for (int i = 0; i < valuesList.size(); ++i) {
                for (int j = i; j < valuesList.size(); ++j) {
                    int check = valuesList.get(i).compareTo(valuesList.get(j));
                    if (check < 0) {
                        context.write(new Text(valuesList.get(i).toString() + ',' + valuesList.get(j).toString()),
                                new Text(key.toString()));
                    }

                }
            }
        }
    }

    public static class CountTriangleMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context)
                throws IOException, InterruptedException {
            String[] pair = value.toString().split("\\s+");
            if (pair.length > 1) {
                context.write(new Text(pair[0]), new Text(pair[1]));
            }
        }
    }

    public static class CountTriangleReducer extends Reducer<Text, Text, LongWritable, LongWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values,
                Reducer<Text, Text, LongWritable, LongWritable>.Context context)
                throws IOException, InterruptedException {
            Set<String> valueSet = new LinkedHashSet<String>();
            long countTriangleCandidates = 0;
            boolean isClosed = false;

            for (Text value : values) {
                valueSet.add(value.toString());
            }

            // check if closed by checking the $ from previous reduce
            for (String value : valueSet) {
                if (!value.equals("$")) {
                    ++countTriangleCandidates;
                } else {
                    isClosed = true;
                }
            }

            // If Closed and count > 0 = closed triplet
            if (isClosed) {
                context.write(new LongWritable(0), new LongWritable(countTriangleCandidates));
            }
        }
    }

    public static class SumTriangleMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, LongWritable, LongWritable>.Context context)
                throws IOException, InterruptedException {

            String[] pair = value.toString().split("\\s+");
            if (pair.length > 1) {
                context.write(new LongWritable(0), new LongWritable(Long.parseLong(pair[1])));
            }

        }
    }

    public static class SumTriangleReducer extends Reducer<LongWritable, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values,
                Reducer<LongWritable, LongWritable, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {

            long sum = 0;
            for (LongWritable value : values) {
                sum += value.get();
            }

            context.write(new Text("Result"), new LongWritable(sum));

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.setInt("mapreduce.task.timeout", 6000000);

        Job preprocessingJob = Job.getInstance(conf, "Preprocessing");
        preprocessingJob.setJarByClass(TriangleCount.class);
        preprocessingJob.setMapperClass(PreprocessorMapper.class);
        preprocessingJob.setReducerClass(PreprocessorReducer.class);

        preprocessingJob.setMapOutputKeyClass(LongWritable.class);
        preprocessingJob.setMapOutputValueClass(LongWritable.class);

        preprocessingJob.setOutputKeyClass(Text.class);
        preprocessingJob.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(preprocessingJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(preprocessingJob, new Path(args[1] + "/1"));

        Job countJob = Job.getInstance(conf, "CountTriangle");
        countJob.setJarByClass(TriangleCount.class);
        countJob.setMapperClass(CountTriangleMapper.class);
        countJob.setReducerClass(CountTriangleReducer.class);

        countJob.setMapOutputKeyClass(Text.class);
        countJob.setMapOutputValueClass(Text.class);

        countJob.setOutputKeyClass(LongWritable.class);
        countJob.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(countJob, new Path(args[1] + "/1"));
        FileOutputFormat.setOutputPath(countJob, new Path(args[1] + "/2"));

        Job sumJob = Job.getInstance(conf, "SumTriangle");
        sumJob.setNumReduceTasks(1);
        sumJob.setJarByClass(TriangleCount.class);
        sumJob.setMapperClass(SumTriangleMapper.class);
        sumJob.setReducerClass(SumTriangleReducer.class);

        sumJob.setMapOutputKeyClass(LongWritable.class);
        sumJob.setMapOutputValueClass(LongWritable.class);

        sumJob.setOutputKeyClass(Text.class);
        sumJob.setOutputValueClass(LongWritable.class);

        FileInputFormat.addInputPath(sumJob, new Path(args[1] + "/2"));
        FileOutputFormat.setOutputPath(sumJob, new Path(args[2]));

        int ret = preprocessingJob.waitForCompletion(true) ? 0 : 1;
        if (ret == 0) {
            ret = countJob.waitForCompletion(true) ? 0 : 1;
        }
        if (ret == 0) {
            ret = sumJob.waitForCompletion(true) ? 0 : 1;
        }

        long startTime = System.currentTimeMillis();
        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println("total time - " + Long.toString(estimatedTime));
        System.exit(ret);
    }

}
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
            SortedSet<Long> valuesSet = new TreeSet<>();
            Map<String, String> connectedIDMap = new HashMap<>();
            Map<String, String> candidateMap = new HashMap<>();

            // iterate value
            while (values.iterator().hasNext()) {
                LongWritable value = values.iterator().next();

                valuesSet.add(value.get());

                String connectedID = key.toString() + ',' + value.get();

                if (!connectedIDMap.containsKey(connectedID)) {
                    connectedIDMap.put(connectedID, "$");
                }

                context.write(new Text(connectedID), new Text("$"));
            }

            // convert set to list for iterating through possible candidates
            List<Long> valuesList = new ArrayList<Long>(valuesSet);

            // generate all triangle candidate
            for (int i = 0; i < valuesList.size(); i++) {
                for (int j = i + 1; j < valuesList.size(); j++) {
                    if (valuesList.get(i) != valuesList.get(j)) {
                        String connectedNodes = valuesList.get(i).toString() + ',' + valuesList.get(j).toString();

                        // put to candidateMap if not written yet
                        if (!candidateMap.containsKey(connectedNodes)) {
                            candidateMap.put(connectedNodes, key.toString());

                            // write to output
                            context.write(new Text(connectedNodes), new Text(key.toString()));
                        }
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
            long count = 0;
            boolean isClosed = false;

            while (values.iterator().hasNext()) {
                Text value = values.iterator().next();
                valueSet.add(value.toString());
            }

            // check if closed by checking the $ from previous reduce
            for (String value : valueSet) {
                if (value.equals("$")) {
                    isClosed = true;
                } else {
                    count++;
                }
            }

            // If Closed and count > 0 = closed triplet
            if (isClosed && count > 0) {
                context.write(new LongWritable(0), new LongWritable(count));
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
                context.write(new LongWritable(Long.parseLong(pair[0])), new LongWritable(Long.parseLong(pair[1])));
            }

        }
    }

    public static class SumTriangleReducer extends Reducer<LongWritable, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(LongWritable key, Iterable<LongWritable> values,
                Reducer<LongWritable, LongWritable, Text, LongWritable>.Context context)
                throws IOException, InterruptedException {

            long sum = 0;
            while (values.iterator().hasNext()) {
                LongWritable value = values.iterator().next();
                sum += value.get();
            }
            context.write(new Text("Triangle Count: "), new LongWritable(sum));

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

        long startTime = System.currentTimeMillis();
        int ret = preprocessingJob.waitForCompletion(true) ? 0 : 1;
        if (ret == 0) {
            ret = countJob.waitForCompletion(true) ? 0 : 1;
        }
        if (ret == 0) {
            ret = sumJob.waitForCompletion(true) ? 0 : 1;
        }

        long estimatedTime = System.currentTimeMillis() - startTime;
        System.out.println("total time - " + Long.toString(estimatedTime));
        System.exit(ret);
    }

}
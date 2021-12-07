import java.io.IOException;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

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
                for (int j = i; j < valuesList.size(); j++) {
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

        }
    }

    public static class SumTriangleMapper extends Mapper<LongWritable, Text, LongWritable, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value,
                Mapper<LongWritable, Text, LongWritable, LongWritable>.Context context)
                throws IOException, InterruptedException {

        }
    }

    public static class SumTriangleReducer extends Reducer<LongWritable, LongWritable, Text, LongWritable> {
        @Override
        protected void reduce(LongWritable arg0, Iterable<LongWritable> arg1,
                Reducer<LongWritable, LongWritable, Text, LongWritable>.Context arg2)
                throws IOException, InterruptedException {

        }
    }

    public static void main(String[] args) {
        Configuration conf = new Configuration();

    }
}
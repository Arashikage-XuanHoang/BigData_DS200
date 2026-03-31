import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Bai04 {

    // ========================
    // MAPPER USERS
    // ========================
    public static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",\\s*");
            if (parts.length < 3) return;

            String userID = parts[0];
            String age = parts[2];

            context.write(new Text(userID), new Text("AGE:" + age));
        }
    }

    // ========================
    // MAPPER RATINGS
    // ========================
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",\\s*");
            if (parts.length < 3) return;

            context.write(
                new Text(parts[0]),
                new Text("RATING:" + parts[1] + ":" + parts[2])
            );
        }
    }

    // ========================
    // REDUCER 1: JOIN + AGE GROUP
    // ========================
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {

        private String getAgeGroup(int age) {
            if (age <= 18) return "0-18";
            else if (age <= 35) return "18-35";
            else if (age <= 50) return "35-50";
            else return "50+";
        }

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            int age = -1;
            List<String> ratings = new ArrayList<>();

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("AGE:")) {
                    age = Integer.parseInt(v.substring(4));
                } else {
                    ratings.add(v.substring(7));
                }
            }

            if (age == -1) return;

            String group = getAgeGroup(age);

            for (String r : ratings) {
                String[] p = r.split(":");
                context.write(new Text(p[0]), new Text(group + ":" + p[1]));
            }
        }
    }

    // ========================
    // MAPPER MOVIES
    // ========================
    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",", 2);
            if (parts.length < 2) return;

            String movieID = parts[0];
            String rest = parts[1];

            int lastComma = rest.lastIndexOf(",");
            if (lastComma == -1) return;

            String title = rest.substring(0, lastComma);

            context.write(new Text(movieID), new Text("TITLE:" + title));
        }
    }

    // ========================
    // MAPPER JOB2 INPUT
    // ========================
    public static class PassMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\\t");
            if (parts.length < 2) return;

            context.write(new Text(parts[0]), new Text(parts[1]));
        }
    }

    // ========================
    // REDUCER 2: JOIN MOVIE
    // ========================
    public static class JoinMovieReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String title = "";
            List<String> list = new ArrayList<>();

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("TITLE:")) {
                    title = v.substring(6);
                } else {
                    list.add(v);
                }
            }

            if (title.isEmpty()) title = key.toString();

            for (String v : list) {
                context.write(new Text(title), new Text(v));
            }
        }
    }

    // ========================
    // FINAL REDUCER
    // ========================
    public static class AvgReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            Map<String, Float> sum = new HashMap<>();
            Map<String, Integer> count = new HashMap<>();

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                if (parts.length < 2) continue;

                String group = parts[0];
                float rating = Float.parseFloat(parts[1]);

                sum.put(group, sum.getOrDefault(group, 0f) + rating);
                count.put(group, count.getOrDefault(group, 0) + 1);
            }

            String[] groups = {"0-18", "18-35", "35-50", "50+"};
            StringBuilder result = new StringBuilder();

            for (String g : groups) {
                if (count.containsKey(g)) {
                    float avg = sum.get(g) / count.get(g);
                    result.append(g).append(": ")
                          .append(String.format("%.2f", avg)).append("   ");
                } else {
                    result.append(g).append(": NA   ");
                }
            }

            context.write(key, new Text(result.toString()));
        }
    }

    // ========================
    // MAIN
    // ========================
    public static void main(String[] args) throws Exception {

        if (args.length < 7) {
            System.err.println("Usage: Bai04 <ratings1> <ratings2> <users> <movies> <out1> <out2> <final>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        // JOB 1
        Job job1 = Job.getInstance(conf, "Join Age");
        job1.setJarByClass(Bai04.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[2]), TextInputFormat.class, UserMapper.class);

        job1.setReducerClass(JoinReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job1, new Path(args[4]));

        if (!job1.waitForCompletion(true)) System.exit(1);

        // JOB 2
        Job job2 = Job.getInstance(conf, "Join Movie");
        job2.setJarByClass(Bai04.class);

        MultipleInputs.addInputPath(job2, new Path(args[4]), TextInputFormat.class, PassMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, MovieMapper.class);

        job2.setReducerClass(JoinMovieReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job2, new Path(args[5]));

        if (!job2.waitForCompletion(true)) System.exit(1);

        // JOB 3
        Job job3 = Job.getInstance(conf, "Final Avg");
        job3.setJarByClass(Bai04.class);

        job3.setMapperClass(PassMapper.class);
        job3.setReducerClass(AvgReducer.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(args[5]));
        FileOutputFormat.setOutputPath(job3, new Path(args[6]));

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
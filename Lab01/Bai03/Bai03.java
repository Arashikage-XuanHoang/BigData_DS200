import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Bai03 {

    // ========================
    // MAPPER USERS
    // ========================
    public static class UserMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",\\s*");
            if (parts.length < 2) return;

            String userID = parts[0].trim();
            String gender = parts[1].trim();

            context.write(new Text(userID), new Text("GENDER:" + gender));
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

            String userID = parts[0].trim();
            String movieID = parts[1].trim();
            String rating = parts[2].trim();

            context.write(new Text(userID), new Text("RATING:" + movieID + ":" + rating));
        }
    }

    // ========================
    // REDUCER 1: JOIN USER + RATING
    // ========================
    public static class JoinUserRatingReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String gender = "";
            List<String> ratings = new ArrayList<>();

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("GENDER:")) {
                    gender = v.substring(7).trim();
                } else if (v.startsWith("RATING:")) {
                    ratings.add(v.substring(7)); // MovieID:Rating
                }
            }

            if (gender.isEmpty()) return;

            for (String r : ratings) {
                String[] parts = r.split(":");
                if (parts.length < 2) continue;

                String movieID = parts[0];
                String rating = parts[1];

                context.write(new Text(movieID), new Text(gender + ":" + rating));
            }
        }
    }

    // ========================
    // MAPPER MOVIES (FIX PARSE)
    // ========================
    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString();

            // split MovieID và phần còn lại
            String[] parts = line.split(",", 2);
            if (parts.length < 2) return;

            String movieID = parts[0].trim();
            String rest = parts[1].trim();

            // tìm dấu , cuối để tách genres
            int lastComma = rest.lastIndexOf(",");
            if (lastComma == -1) return;

            String title = rest.substring(0, lastComma).trim();

            context.write(new Text(movieID), new Text("TITLE:" + title));
        }
    }

    // ========================
    // MAPPER JOB2 INPUT
    // ========================
    public static class GenderRatingMapper extends Mapper<LongWritable, Text, Text, Text> {
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
            List<String> ratings = new ArrayList<>();

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("TITLE:")) {
                    title = v.substring(6).trim();
                } else {
                    ratings.add(v);
                }
            }

            if (title.isEmpty()) return;

            for (String r : ratings) {
                context.write(new Text(title), new Text(r));
            }
        }
    }

    // ========================
    // MAPPER FINAL
    // ========================
    public static class FinalMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\\t");
            if (parts.length < 2) return;

            context.write(new Text(parts[0]), new Text(parts[1]));
        }
    }

    // ========================
    // REDUCER FINAL: AVG
    // ========================
    public static class AvgReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            float maleSum = 0, femaleSum = 0;
            int maleCount = 0, femaleCount = 0;

            for (Text val : values) {
                String[] parts = val.toString().split(":");
                if (parts.length < 2) continue;

                String gender = parts[0].trim();
                float rating = Float.parseFloat(parts[1].trim());

                if (gender.equalsIgnoreCase("M")) {
                    maleSum += rating;
                    maleCount++;
                } else if (gender.equalsIgnoreCase("F")) {
                    femaleSum += rating;
                    femaleCount++;
                }
            }

            float maleAvg = maleCount == 0 ? 0 : maleSum / maleCount;
            float femaleAvg = femaleCount == 0 ? 0 : femaleSum / femaleCount;

            String result = String.format("Male: %.2f, Female: %.2f", maleAvg, femaleAvg);
            context.write(key, new Text(result));
        }
    }

    // ========================
    // MAIN
    // ========================
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();

        // JOB 1
        Job job1 = Job.getInstance(conf, "Join User Rating");
        job1.setJarByClass(Bai03.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]),
                TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]),
                TextInputFormat.class, UserMapper.class);

        job1.setReducerClass(JoinUserRatingReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job1, new Path(args[2]));

        if (!job1.waitForCompletion(true)) System.exit(1);

        // JOB 2
        Job job2 = Job.getInstance(conf, "Join Movie");
        job2.setJarByClass(Bai03.class);

        MultipleInputs.addInputPath(job2, new Path(args[2]),
                TextInputFormat.class, GenderRatingMapper.class);
        MultipleInputs.addInputPath(job2, new Path(args[3]),
                TextInputFormat.class, MovieMapper.class);

        job2.setReducerClass(JoinMovieReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job2, new Path(args[4]));

        if (!job2.waitForCompletion(true)) System.exit(1);

        // JOB 3
        Job job3 = Job.getInstance(conf, "Final Avg");
        job3.setJarByClass(Bai03.class);

        job3.setMapperClass(FinalMapper.class);
        job3.setReducerClass(AvgReducer.class);

        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job3, new Path(args[4]));
        FileOutputFormat.setOutputPath(job3, new Path(args[5]));

        System.exit(job3.waitForCompletion(true) ? 0 : 1);
    }
}
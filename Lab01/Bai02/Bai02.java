import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class Bai02 {

    // =========================
    // MAPPER: MOVIES
    // =========================
    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // split theo ", " hoặc ","
            String[] parts = value.toString().split(",\\s*", 3);
            if (parts.length < 3) return;

            String movieID = parts[0].trim();
            String genres = parts[2].trim();

            context.write(new Text(movieID), new Text("GENRE:" + genres));
        }
    }

    // =========================
    // MAPPER: RATINGS
    // =========================
    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split(",\\s*");
            if (parts.length < 3) return;

            String movieID = parts[1].trim();
            String rating = parts[2].trim();

            context.write(new Text(movieID), new Text("RATING:" + rating));
        }
    }

    // =========================
    // REDUCER 1: JOIN
    // =========================
    public static class JoinReducer extends Reducer<Text, Text, Text, Text> {
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            String genres = "";
            List<String> ratings = new ArrayList<>();

            for (Text val : values) {
                String v = val.toString();

                if (v.startsWith("GENRE:")) {
                    genres = v.substring(6);
                } else if (v.startsWith("RATING:")) {
                    ratings.add(v.substring(7));
                }
            }

            if (genres.isEmpty()) return;

            String[] genreList = genres.split("\\|");

            for (String g : genreList) {
                for (String r : ratings) {
                    context.write(new Text(g), new Text(r));
                }
            }
        }
    }

    // =========================
    // MAPPER 2
    // =========================
    public static class GenreMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\\t");
            if (parts.length < 2) return;

            String genre = parts[0];
            float rating = Float.parseFloat(parts[1]);

            context.write(new Text(genre), new FloatWritable(rating));
        }
    }

    // =========================
    // REDUCER 2: AVG + COUNT
    // =========================
    public static class AvgReducer extends Reducer<Text, FloatWritable, Text, Text> {
        public void reduce(Text key, Iterable<FloatWritable> values, Context context)
                throws IOException, InterruptedException {

            float sum = 0;
            int count = 0;

            for (FloatWritable v : values) {
                sum += v.get();
                count++;
            }

            if (count == 0) return;

            float avg = sum / count;

            String result = String.format("Avg: %.2f, Count: %d", avg, count);
            context.write(key, new Text(result));
        }
    }

    // =========================
    // MAIN
    // =========================
    public static void main(String[] args) throws Exception {

        if (args.length != 5) {
            System.err.println("Usage: Bai02 <movies> <ratings1> <ratings2> <tmp> <final>");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        // =========================
        // JOB 1: JOIN
        // =========================
        Job job1 = Job.getInstance(conf, "Join Movies & Ratings");
        job1.setJarByClass(Bai02.class);

        MultipleInputs.addInputPath(job1, new Path(args[0]),
                TextInputFormat.class, MovieMapper.class);

        MultipleInputs.addInputPath(job1, new Path(args[1]),
                TextInputFormat.class, RatingMapper.class);

        MultipleInputs.addInputPath(job1, new Path(args[2]),
                TextInputFormat.class, RatingMapper.class);

        job1.setReducerClass(JoinReducer.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job1, new Path(args[3]));

        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        // =========================
        // JOB 2: AVG
        // =========================
        Job job2 = Job.getInstance(conf, "Average Rating per Genre");
        job2.setJarByClass(Bai02.class);

        job2.setMapperClass(GenreMapper.class);
        job2.setReducerClass(AvgReducer.class);

        job2.setNumReduceTasks(1); // gom 1 file output

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(FloatWritable.class);

        FileInputFormat.addInputPath(job2, new Path(args[3]));
        FileOutputFormat.setOutputPath(job2, new Path(args[4]));

        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
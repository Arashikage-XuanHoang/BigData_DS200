import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class Bai01 {

    private static String maxMovieTitle = "";
    private static double maxRating = -1.0;

    public static class MovieMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split(",", 2);
            if (parts.length < 2) return;
            String movieId = parts[0].trim();
            String title = parts[1].trim();
            context.write(new Text(movieId), new Text("M:" + title));
        }
    }

    public static class RatingMapper extends Mapper<LongWritable, Text, Text, Text> {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();
            if (line.isEmpty()) return;
            String[] parts = line.split(",");
            if (parts.length < 4) return;
            String movieId = parts[1].trim();
            String ratingStr = parts[2].trim();
            context.write(new Text(movieId), new Text("R:" + ratingStr));
        }
    }

    public static class AverageReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) 
                throws IOException, InterruptedException {

            String movieTitle = null;
            double sum = 0.0;
            int count = 0;

            for (Text val : values) {
                String str = val.toString();
                if (str.startsWith("M:")) {
                    movieTitle = str.substring(2);
                } else if (str.startsWith("R:")) {
                    try {
                        double rating = Double.parseDouble(str.substring(2));
                        sum += rating;
                        count++;
                    } catch (Exception e) {}
                }
            }

            if (movieTitle == null || count == 0) return;

            double average = sum / count;

            context.write(new Text(movieTitle + " AverageRating: " + String.format("%.2f", average) 
                        + " (TotalRatings: " + count + ")"), new Text(""));

            if (count >= 5 && average > maxRating) {
                maxRating = average;
                maxMovieTitle = movieTitle;
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            if (!maxMovieTitle.isEmpty()) {
                context.write(new Text(maxMovieTitle + " is the highest rated movie with an average rating of " 
                            + String.format("%.2f", maxRating) 
                            + " among movies with at least 5 ratings."), new Text(""));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length < 4) {
            System.err.println("Usage: hadoop jar ... Bai01 <movies> <ratings1> <ratings2> <output>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "Movie Average Rating");
        job.setJarByClass(Bai01.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(AverageReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(otherArgs[0]), TextInputFormat.class, MovieMapper.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[1]), TextInputFormat.class, RatingMapper.class);
        MultipleInputs.addInputPath(job, new Path(otherArgs[2]), TextInputFormat.class, RatingMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(otherArgs[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

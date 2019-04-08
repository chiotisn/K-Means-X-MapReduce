import java.io.BufferedReader;
import java.io.FileReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.*;

public class KMeans  extends Configured implements Tool {

    public static ArrayList<Point> centroids = new ArrayList<Point>();
    public static Configuration conf;
    public static Boolean stop = false;
    public static ArrayList<String> centroidsStatus = new ArrayList<String>();

    public static class Point {

	     private double cords[] = new double[2];

       public Point(String line) {
         String x,y;
         x = line.split(",")[0];
         y = line.split(",")[1];
         cords[0] = Double.parseDouble(x);
         cords[1] = Double.parseDouble(y);
		   }

       public double getx() {
         return cords[0];
       }

       public double gety() {
         return cords[1];
       }

		   public static double getDist(Point a, Point b) {
         return Math.sqrt( Math.pow((a.getx() - b.getx()), 2) + Math.pow((a.getx() - b.getx()), 2) );
       }

       public String toString() {
         String point = String.valueOf(cords[0]) + "," + String.valueOf(cords[1]);
         return point;
       }
	  }

    public static class PointsMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void setup(Context context) {

        }

        @Override
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			       int index = -1;
			       double minDist = Double.MAX_VALUE;
             Point p = new Point(value.toString());

             for (int i = 0; i < 3; i++) {
                double dist = Point.getDist(p, centroids.get(i));
                if (dist < minDist) {
                    minDist = dist;
                    index = i;
                }
             }
             context.write(new Text(centroids.get(index).toString()), new Text(p.toString()));
        }

        @Override
        public void cleanup(Context context) throws IOException,InterruptedException {

        }
    }

    public static class PointsReducer extends Reducer<Text, Text, Text, Text> {

          @Override
          public void reduce(Text key,Iterable<Text> values,Context context) throws IOException,InterruptedException {
               double sumx = 0;
               double sumy = 0;
  			       int counter = 0;
               String line, x, y;

               while (values.iterator().hasNext()) {
  				           line = values.iterator().next().toString();
                     String[] str1=line.split(":");
                     x = line.split(",")[0];
                     y = line.split(",")[1];
                     sumx += Double.parseDouble(x);
                     sumy += Double.parseDouble(y);
                     counter ++;
              }
              Point old_centroid = new Point(key.toString());
              double newx = sumx/counter;
              double newy = sumy/counter;
              Point new_centroid = new Point(String.valueOf(newx) + "," + String.valueOf(newy));
              String changed = "not changed";
              if (Point.getDist(new_centroid,old_centroid) > 0.3) {
                changed = "changed";
                centroids.remove(old_centroid);
                centroids.add(new_centroid);
              }
              centroidsStatus.add(changed);
              context.write(new Text(new_centroid.toString()),new Text("|" + String.valueOf(counter)));
        }
    }

    @Override
    public int run(String[] args) throws Exception {

      Job job = Job.getInstance(conf, "KMeans");
      FileSystem fs=FileSystem.get(conf);
      job.setJarByClass(KMeans.class);
      job.setMapperClass(PointsMapper.class);
      job.setReducerClass(PointsReducer.class);
      job.setOutputKeyClass(Text.class);
      job.setOutputValueClass(Text.class);
      job.setNumReduceTasks(1);
      FileInputFormat.addInputPath(job, new Path(args[0]));
      Path output = new Path (args[1]);
      fs.delete(output,true);
      FileOutputFormat.setOutputPath(job, output);

      return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {

        //3 random centroids
        centroids.add(new Point("5.938284859084138,10.439430174938032"));
        centroids.add(new Point("70.818654512579638,29.559624209285044"));
        centroids.add(new Point("25.77863208190015,1.058336941667927"));

        conf = new Configuration();

        int i = 0;
        int exit = 1;
        do {
          exit ^= ToolRunner.run(conf, new KMeans(), args);
          i++;
          if (!centroidsStatus.contains("changed")) {
            stop = true;
          }
          centroidsStatus.clear();
        } while (exit == 1 && i < 6 && !stop); //maximum iterations: 6
        System.out.println("Number of iterations: " + String.valueOf(i));
    }

}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.*;
import java.io.IOException;
import java.util.*;
import java.nio.charset.StandardCharsets;

/*
Execution Guide:
hadoop com.sun.tools.javac.Main ScoreComp.java
jar cf ScoreComp.jar ScoreComp*.class
hadoop jar ScoreComp.jar ScoreComp
hadoop fs -cat scores/part-r-00000
*/

public class ScoreComp 
{
    /* input:  <Character, Number>
     * output: <Character, Number>
     */
    public static class Map extends Mapper<Object, Text, Text, DoubleWritable> 
    {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            try
            {
                if(value.toString().contains("Num")) // remove header
                    return;
                else 
                {
                    String record = value.toString();
                    String[] parts = record.split(" "); // just split the lines into key and value

                    // create key-value pairs from each line
                    context.write(new Text(parts[0]), new DoubleWritable(Double.parseDouble(parts[1])));
                }
            }
            catch (Exception e) 
            {
                e.printStackTrace();
            }
        }
    }

    /* input:  <Character, Number>
     * output: <Character, Score>
     */
    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
        {
            double pos = 0.0;
            double neg = 0.0;

            // for every value of a unique key...
            for(DoubleWritable value : values)
            {
                // retrieve the positive number and calculate the sum of the two negative numbers
                if(value.get() < 0)
                    neg += value.get();
                else
                    pos = value.get();
            }

            // calculate the score based on the values of each key (using explicit type casting)
            double result = (double) pos / (-1 * neg);

            // create key-value pairs for each key with its score
            context.write(key, new DoubleWritable(result));
        }
    }


    public static void main(String[] args) throws Exception
    {
        // set the paths of the input and output directories in the HDFS
        Path input_dir = new Path("input");
        Path output_dir = new Path("scores");

        // in case the output directory already exists, delete it
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output_dir))
            fs.delete(output_dir, true);

        // configure the MapReduce job
        Job scorecomp_job = Job.getInstance(conf, "Score Computation");
        scorecomp_job.setJarByClass(ScoreComp.class);
        scorecomp_job.setMapperClass(Map.class);
        scorecomp_job.setReducerClass(Reduce.class);    
        scorecomp_job.setMapOutputKeyClass(Text.class);
        scorecomp_job.setMapOutputValueClass(DoubleWritable.class);
        scorecomp_job.setOutputKeyClass(Text.class);
        scorecomp_job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(scorecomp_job, input_dir);
        FileOutputFormat.setOutputPath(scorecomp_job, output_dir);
        scorecomp_job.waitForCompletion(true);
    }
}
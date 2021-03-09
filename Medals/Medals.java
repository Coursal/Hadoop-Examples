import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
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
hadoop com.sun.tools.javac.Main Medals.java
jar cf Medals.jar Medals*.class
hadoop jar Medals.jar Medals
hadoop fs -cat medals/part-r-00000
*/

public class Medals 
{
    /* input:  <byte_offset, line_of_dataset>
     * output: <(name,sex,country,sport), (gold#silver#bronze)>
     */
    public static class Map extends Mapper<Object, Text, Text, Text> 
    {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            try
            {
                if(value.toString().contains("nationality")) // remove header
                    return;
                else 
                {
                    String record = value.toString();
                    String[] columns = record.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

                    // extract athlete's main info
                    String name = columns[1];
                    String sex = columns[3];
                    String country = columns[2];
                    String sport = columns[7];

                    // extract athlete's stat info
                    String gold = columns[8];
                    String silver = columns[9];
                    String bronze = columns[10];

                    // set the main info as key and the stat info as value
                    context.write(new Text(name + "," + sex + "," + country + "," + sport), new Text(gold + "#" + silver + "#" + bronze));
                }
            }
            catch (Exception e) 
            {
                e.printStackTrace();
            }
        }
    }

    /* input:  <(name,sex,country,sport), (gold#silver#bronze)>
     * output: <(NULL, (name,sex,,country,sport,,golds,silvers,bronzes)>
     */
    public static class Reduce extends Reducer<Text, Text, NullWritable, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            // extract athlete's main info
            String[] athlete_info = key.toString().split(",");
            String name = athlete_info[0];
            String sex = athlete_info[1];
            String country = athlete_info[2];
            String sport = athlete_info[3];
            
            int gold_cnt = 0;
            int silver_cnt = 0;
            int bronze_cnt = 0;

            // for a single athlete, compute their stats...
            for(Text value : values)
            {
                String[] medals = value.toString().split("#");
                
                gold_cnt += Integer.parseInt(medals[0]);
                silver_cnt += Integer.parseInt(medals[1]);
                bronze_cnt += Integer.parseInt(medals[2]);
            }

            context.write(NullWritable.get(), new Text(name + "," + sex + "," + "," + country + "," + sport + "," + String.valueOf(gold_cnt) + "," + String.valueOf(silver_cnt) + "," + String.valueOf(bronze_cnt)));
        }
    }


    public static void main(String[] args) throws Exception
    {
        // set the paths of the input and output directories in the HDFS
        Path input_dir = new Path("olympic_stats");
        Path output_dir = new Path("medals");

        // in case the output directory already exists, delete it
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output_dir))
            fs.delete(output_dir, true);

        // configure the MapReduce job
        Job medals_job = Job.getInstance(conf, "Medals Counter");
        medals_job.setJarByClass(Medals.class);
        medals_job.setMapperClass(Map.class);
        medals_job.setReducerClass(Reduce.class);    
        medals_job.setMapOutputKeyClass(Text.class);
        medals_job.setMapOutputValueClass(Text.class);
        medals_job.setOutputKeyClass(NullWritable.class);
        medals_job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(medals_job, input_dir);
        FileOutputFormat.setOutputPath(medals_job, output_dir);
        medals_job.waitForCompletion(true);
    }
}
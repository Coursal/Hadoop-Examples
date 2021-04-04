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
javac -classpath "$(yarn classpath)" -d . AvgPrice.java 
jar -cvf AvgPrice.jar -C . .
hadoop jar AvgPrice.jar AvgPrice
hadoop fs -cat average_prices/part-r-00000
*/

public class AvgPrice 
{
    /* input:  <byte_offset, line_of_dataset>
     * output: <zipcode, price>
     */
    public static class Map extends Mapper<Object, Text, Text, DoubleWritable> 
    {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            try 
            { 
                if(value.toString().contains("Address")) // remove header
                    return;
                else 
                {
                    String[] data = value.toString().split(",");
                    String zipcode = data[3];
                    DoubleWritable price = new DoubleWritable(Double.parseDouble(data[4]));

                    context.write(new Text(zipcode), price);
                }
            } 
            catch (Exception e) 
            {
                e.printStackTrace();
            }
        }
    }

    /* input:  <zipcode, price>
     * output: <zipcode, avg_price>
     */
    public static class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {
        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException 
        {
            double sum = 0;
            int num_of_prices = 0;

            for(DoubleWritable value : values)
            {
                sum += value.get();
                num_of_prices++;
            }

            context.write(key, new DoubleWritable((double) sum / num_of_prices));
        }
    }


    public static void main(String[] args) throws Exception
    {
        // set the paths of the input and output directories in the HDFS
        Path input_dir = new Path("address_book");
        Path output_dir = new Path("average_prices");

        // in case the output directory already exists, delete it
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output_dir))
            fs.delete(output_dir, true);

        // configure the MapReduce job
        Job AvgPrice_job = Job.getInstance(conf, "Average Price");
        AvgPrice_job.setJarByClass(AvgPrice.class);
        AvgPrice_job.setMapperClass(Map.class);
        AvgPrice_job.setCombinerClass(Reduce.class);
        AvgPrice_job.setReducerClass(Reduce.class);    
        AvgPrice_job.setMapOutputKeyClass(Text.class);
        AvgPrice_job.setMapOutputValueClass(DoubleWritable.class);
        AvgPrice_job.setOutputKeyClass(Text.class);
        AvgPrice_job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(AvgPrice_job, input_dir);
        FileOutputFormat.setOutputPath(AvgPrice_job, output_dir);
        AvgPrice_job.waitForCompletion(true);
    }
}
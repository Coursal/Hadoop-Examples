import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
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
hadoop com.sun.tools.javac.Main MaxTemp.java
jar cf MaxTemp.jar MaxTemp*.class
hadoop jar MaxTemp.jar MaxTemp
hadoop fs -cat temp_out/part-r-00000
*/

public class MaxTemp 
{
	/* input:  <byte_offset, line_of_dataset>
     * output: <City, Temperature>
     */
	public static class Map extends Mapper<Object, Text, Text, IntWritable> 
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			try
	    	{
	    		if(value.toString().contains("Temperature")) // remove header
	                return;
	            else 
	            {
					String record = value.toString();
					String[] parts = record.split(", ");

					context.write(new Text(parts[0]), new IntWritable(Integer.parseInt(parts[1])));
				}
			}
        	catch (Exception e) 
	        {
	            e.printStackTrace();
	        }
		}
	}

	/* input:  <City, Temperature>
     * output: <City, Max Temperature>
     */
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
	    {
			int max_value = 0;
			
            for(IntWritable value : values)
            {
            	if(value.get() > max_value)
            		max_value = value.get();
            }

			context.write(key, new IntWritable(max_value));
		}
	}


	public static void main(String[] args) throws Exception
	{
		// set the paths of the input and output directories in the HDFS
		Path input_dir = new Path("temperatures");
		Path output_dir = new Path("temp_out");

		// in case the output directory already exists, delete it
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output_dir))
            fs.delete(output_dir, true);

        // configure the MapReduce job
		Job maxtemp_job = Job.getInstance(conf, "Max Temperature");
        maxtemp_job.setJarByClass(MaxTemp.class);
        maxtemp_job.setMapperClass(Map.class);
        maxtemp_job.setCombinerClass(Reduce.class);
        maxtemp_job.setReducerClass(Reduce.class);    
        maxtemp_job.setMapOutputKeyClass(Text.class);
        maxtemp_job.setMapOutputValueClass(IntWritable.class);
        maxtemp_job.setOutputKeyClass(Text.class);
        maxtemp_job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(maxtemp_job, input_dir);
        FileOutputFormat.setOutputPath(maxtemp_job, output_dir);
        maxtemp_job.waitForCompletion(true);
	}
}
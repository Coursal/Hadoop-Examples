import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
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
javac -classpath "$(yarn classpath)" -d . SymDiff.java 
jar -cvf SymDiff.jar -C . .
hadoop jar SymDiff.jar SymDiff
hadoop fs -cat set_output/part-r-00000
*/

public class SymDiff 
{
    /* input:  <byte_offset, line_of_dataset>
     * output: <ID, file>
     */
    public static class Map extends Mapper<LongWritable, Text, Text, Text> 
    {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            String[] line = value.toString().split(" ");    // split each line to two columns
            
            // if the first column consists of integers, put the ID from the 2nd column as the key
            // and set "B" as the value to imply that the particular ID was found on the second file
            // else, put the ID from the first column as the key
            // and set "A" as the value to imply that the particular ID was found on the first file
            if(line[0].matches("\\d+"))     // (lazy way to see if the first string is an int without throwing an exception)
                context.write(new Text(line[1]), new Text("B"));
            else
                context.write(new Text(line[0]), new Text("A"));
        }
    }

    /* input: <ID, file>
     * output: <ID, "">
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text>
    {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            HashSet<String> list_of_files = new HashSet<String>();

            // store all the instances of "A" and "B" for each ID in a HashSet with unique values
            for(Text value : values)
                list_of_files.add(value.toString());
            
            // only write the IDs which they values only contain "A" or "B" (and not both) on their set
            // alternatively, use the following (more verbose) if statement
            // if(list_of_files.contains("A") && !list_of_files.contains("B") || (!list_of_files.contains("A") && list_of_files.contains("B")))
            if(list_of_files.size() == 1)
                context.write(key, new Text(""));
        }
    }


    public static void main(String[] args) throws Exception
    {
        // set the paths of the input and output directories in the HDFS
        Path input_dir = new Path("set_input");
        Path output_dir = new Path("set_output");

        // in case the output directory already exists, delete it
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output_dir))
            fs.delete(output_dir, true);

        // configure the MapReduce job
        Job job = Job.getInstance(conf, "Symmetric Difference");
        job.setJarByClass(SymDiff.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);    
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, input_dir);
        FileOutputFormat.setOutputPath(job, output_dir);
        job.waitForCompletion(true);
    }
}
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
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
hadoop com.sun.tools.javac.Main PatientFilter.java
jar cf PatientFilter.jar PatientFilter*.class
hadoop jar PatientFilter.jar PatientFilter
hadoop fs -cat patients_out/part-r-00000
*/

public class PatientFilter 
{
    /* input:  <byte_offset, line_of_dataset>
     * output: <patient_cycle, (ID, counceling)>
     */
    public static class Map extends Mapper<LongWritable, Text, IntWritable, Text> 
    {
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {
            try 
            { 
                if(value.toString().contains("PatientCycleNum"))   // remove header
                    return;
                else 
                {
                    String[] columns = value.toString().split(", ");   // split the columns

                    String id = columns[0];
                    int patient_cycle = Integer.parseInt(columns[1]);
                    String counceling = columns[2];

                    // rearrange the columns to put the cycles as key and the rest as a composite value
                    context.write(new IntWritable(patient_cycle), new Text(id + "_" + counceling));
                }
            } 
            catch (Exception e) 
            {
                e.printStackTrace();
            }
        }
    }

    /* input: <patient_cycle, (ID, counceling)>
     * output: <ID, (patient_cycle, counceling)>
     */
    public static class Reduce extends Reducer<IntWritable, Text, Text, Text>
    {
        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            // for every value grouped by key...
            for(Text value : values)
            {
                String[] split_value = value.toString().split("_");     // split the composite value

                if(key.get() == 1)      // check if the key/patient cycle in this reducer is 1
                {
                    if(split_value[1].equals("Yes"))    // check if counseling for this record is equal to "Yes"
                    {
                        // for patients with just 1 cycle, only store the records that have "counseling" equal to "Yes"
                        context.write(new Text(split_value[0]), new Text(Integer.toString(key.get()) + " " + split_value[1]));
                    }
                }
                else    // store all the other records as well
                    context.write(new Text(split_value[0]), new Text(Integer.toString(key.get()) + " " + split_value[1]));
            }
        }
    }


    public static void main(String[] args) throws Exception
    {
        // set the paths of the input and output directories in the HDFS
        Path input_dir = new Path("patients");
        Path output_dir = new Path("patients_out");

        // in case the output directory already exists, delete it
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output_dir))
            fs.delete(output_dir, true);

        // configure the MapReduce job
        Job job = Job.getInstance(conf, "Patient Filter");
        job.setJarByClass(PatientFilter.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);    
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        TextInputFormat.addInputPath(job, input_dir);
        TextOutputFormat.setOutputPath(job, output_dir);
        job.waitForCompletion(true);
    }
}
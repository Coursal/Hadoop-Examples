package readfolderfiles;

import readfolderfiles.WholeFileInputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.InputSplit;
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
javac -classpath "$(yarn classpath)" -d . ReadFolderFiles.java WholeFileInputFormat.java WholeFileRecordReader.java 
jar -cvf ReadFolderFiles.jar -C . .
hadoop jar ReadFolderFiles.jar readfolderfiles.ReadFolderFiles
hadoop fs -cat dir_out/part-r-00000
*/

public class ReadFolderFiles 
{
    /* input:  <null, file_content>
     * output: <filename, file_content>
     */
    public static class Map extends Mapper<NullWritable, BytesWritable, Text, Text> 
    {
        private Text filename_key;
        private Text file_contents_value;

        protected void setup(Context context) throws IOException, InterruptedException 
        {
            // get the name of each scanned file to be used as key for the map output pairs
            InputSplit split = context.getInputSplit();
            Path path = ((FileSplit) split).getPath();
            filename_key = new Text(path.getName());

            // create object for the file's contents to be put as value for the map output pairs
            file_contents_value = new Text();   
        }

        public void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException 
        {
            // get the byte array with the file contents, convert it to String, replace the newlines `\n` with 3 dashes, 
            // and set that Text object as the value of the key-value Map output pairs
            file_contents_value.set(new String(value.getBytes(), StandardCharsets.UTF_8).replace("\n", " --- "));
            context.write(filename_key, file_contents_value);
        }
    }

    public static void main(String[] args) throws Exception
    {
        // set the paths of the input and output directories in the HDFS
        Path input_dir = new Path("alphabet_dir");
        Path output_dir = new Path("dir_out");

        // in case the output directory already exists, delete it
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output_dir))
            fs.delete(output_dir, true);

        // configure the MapReduce job
        Job job = Job.getInstance(conf, "Read Folder Files");
        job.setJarByClass(ReadFolderFiles.class);
        job.setMapperClass(Map.class);  
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setInputFormatClass(WholeFileInputFormat.class);
        FileInputFormat.addInputPath(job, input_dir);
        FileOutputFormat.setOutputPath(job, output_dir);
        job.waitForCompletion(true);
    }
}
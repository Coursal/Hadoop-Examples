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
hadoop com.sun.tools.javac.Main OldestTree.java
jar cf OldestTree.jar OldestTree*.class
hadoop jar OldestTree.jar OldestTree
hadoop fs -cat oldest_tree/part-r-00000
*/

public class OldestTree
{
    /* input:  <byte_offset, line_of_dataset>
     * output: <NULL, (district, tree_age)>
     */
    public static class Map extends Mapper<Object, Text, NullWritable, Text> 
    {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            try
            {
                if(value.toString().contains("age_of_tree")) // remove header
                    return;
                else 
                {
                    String row = value.toString();

                    String[] columns = row.split(",");      // split each row by the delimiter
                    String district_name = columns[0];
                    String tree_age = columns[1];

                    // set NULL as key for the generated key-value pairs aimed at the reducers
                    // and set the district with each of its trees age as a composite value,
                    // with the '@' character as a delimiter
                    context.write(NullWritable.get(), new Text(district_name + '@' + tree_age));
                }
            }
            catch (Exception e) 
            {
                e.printStackTrace();
            }
        }
    }

    /* input: <NULL, (district, tree_age)>
     * output: <district_with_oldest_tree, max_tree_age>
     */
    public static class Reduce extends Reducer<NullWritable, Text, Text, IntWritable>
    {
        public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            String district_with_oldest_tree = "";
            int max_tree_age = 0;

            // for all the values with the same (NULL) key,
            // aka all the key-value pairs...
            for(Text value : values)
            {
                // split the composite value by the '@' delimiter
                String[] splitted_values = value.toString().split("@");
                String district_name = splitted_values[0];
                int tree_age = Integer.parseInt(splitted_values[1]);

                // find the district with the oldest tree
                if(tree_age > max_tree_age)
                {
                    district_with_oldest_tree = district_name;
                    max_tree_age = tree_age;
                }
            }

            // output the district (key) with the oldest tree's year of planting (value)
            // to the output directory
            context.write(new Text(district_with_oldest_tree), new IntWritable(max_tree_age));
        }
    }

    public static void main(String[] args) throws Exception
    {
        // set the paths of the input and output directories in the HDFS
        Path input_dir = new Path("trees");
        Path output_dir = new Path("oldest_tree");

        // in case the output directory already exists, delete it
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output_dir))
            fs.delete(output_dir, true);

        // configure the MapReduce job
        Job oldesttree_job = Job.getInstance(conf, "Oldest Tree");
        oldesttree_job.setJarByClass(OldestTree.class);
        oldesttree_job.setMapperClass(Map.class);
        oldesttree_job.setReducerClass(Reduce.class);    
        oldesttree_job.setMapOutputKeyClass(NullWritable.class);
        oldesttree_job.setMapOutputValueClass(Text.class);
        oldesttree_job.setOutputKeyClass(Text.class);
        oldesttree_job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(oldesttree_job, input_dir);
        FileOutputFormat.setOutputPath(oldesttree_job, output_dir);
        oldesttree_job.waitForCompletion(true);
    }
}
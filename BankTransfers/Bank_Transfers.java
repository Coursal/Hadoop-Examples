import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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
hadoop com.sun.tools.javac.Main Bank_Transfers.java
jar cf Bank_Transfers.jar Bank_Transfers*.class
hadoop jar Bank_Transfers.jar Bank_Transfers
hadoop fs -cat bank_output/part-r-00000
*/

public class Bank_Transfers 
{
    /* input:  <byte_offset, line_of_dataset>
     * output: <bank, num_of_transfers@transfer_amount>
     */
    public static class Map extends Mapper<Object, Text, Text, Text> 
    {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            try
            {
                if(value.toString().contains("Amount")) // remove header
                    return;
                else 
                {
                    // tokenize each line of the dataset
                    String line = value.toString();
                    String[] columns = line.split(" ");
              
                    // set the key-value pair with the name of the bank (aka the 1st column of the line) as key
                    // and a composite value of (num_of_transfers, transfer_amount)
                    // (with the number of transfers initialized to 1 for every Mapper)
                    // using the '@' character as a delimiter between them
                    context.write(new Text(columns[0]), new Text("1" + "@" + columns[2]));
                }
            }
            catch (Exception e) 
            {
                e.printStackTrace();
            }
        }
    }

    /* input:  <bank, num_of_transfers@transfer_amount>
     * output: <bank, num_of_transfers,sum_of_transfers>
     */
    public static class Reduce extends Reducer<Text, Text, Text, Text>  
    {
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            // initialize the counters at 0
            int num_of_transfers = 0;
            int sum_of_transfers = 0;

            // for each bank...
            for(Text value : values)
            {
                // split the composite values by the '@' delimiter...
                String[] splitted_value = value.toString().split("@");

                // calculate the number and sum of the transfers...
                num_of_transfers += Integer.parseInt(splitted_value[0]);
                sum_of_transfers += Integer.parseInt(splitted_value[1]);
            }

            // and write the results in disk
            context.write(key, new Text(String.valueOf(num_of_transfers) + ' ' + String.valueOf(sum_of_transfers)));
        }
    }


    public static void main(String[] args) throws Exception 
    {
        // set the paths of the input and output directories in the HDFS
        Path input_dir = new Path("bank_dataset");
        Path output_dir = new Path("bank_output");

        // in case the output directory already exists, delete it
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output_dir))
            fs.delete(output_dir, true);

        // configure the MapReduce job
        Job bank_job = Job.getInstance(conf, "Bank Transfers");
        bank_job.setJarByClass(Bank_Transfers.class);
        bank_job.setMapperClass(Map.class);
        bank_job.setReducerClass(Reduce.class);    
        bank_job.setMapOutputKeyClass(Text.class);
        bank_job.setMapOutputValueClass(Text.class);
        bank_job.setOutputKeyClass(Text.class);
        bank_job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(bank_job, input_dir);
        FileOutputFormat.setOutputPath(bank_job, output_dir);
        bank_job.waitForCompletion(true);
    }
}

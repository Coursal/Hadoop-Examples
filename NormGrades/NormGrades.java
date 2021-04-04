import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
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
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Cluster;

import java.io.*;
import java.io.IOException;
import java.util.*;
import java.nio.charset.StandardCharsets;

/*
Execution Guide:
javac -classpath "$(yarn classpath)" -d . NormGrades.java 
jar -cvf NormGrades.jar -C . .
hadoop jar NormGrades.jar NormGrades
hadoop fs -cat normalized_grades/part-r-00000
*/

public class NormGrades
{
    public static enum Global_Counters 
    {
        MAX_GRADE,
        MIN_GRADE
    }

    /* input:  <byte_offset, line_of_tweet>
     * output: <student, grade>
     */
    public static class Map_Normalize extends Mapper<Object, Text, Text, IntWritable> 
    {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            try
            {
                if(value.toString().contains("Name")) // remove header
                    return;
                else 
                {
                    String line = value.toString();
                    String[] columns = line.split(",");

                    int student_grade = Integer.parseInt(columns[1]);

                    int max_grade = Math.toIntExact(context.getCounter(Global_Counters.MAX_GRADE).getValue());
                    int min_grade = Math.toIntExact(context.getCounter(Global_Counters.MIN_GRADE).getValue());

                    // in order to find the maximum grade, we first set the max grade counter to 0
                    // by "increasing" it to the negative value of itself, and then increment by
                    // the new found maximum grade
                    if(student_grade > max_grade)
                    {
                        context.getCounter(Global_Counters.MAX_GRADE).increment(max_grade * (-1));  
                        context.getCounter(Global_Counters.MAX_GRADE).increment(student_grade);
                    }

                    // in order to find the minimum grade, we first set the min grade counter to 0
                    // by "increasing" it to the negative value of itself, and then increment by
                    // the new found minimum grade
                    // the contents on this if statement will be accessed at least once in order to
                    // make sure that the min grade counter value is certainly higher than 0
                    if((student_grade < min_grade) || (min_grade == 0))
                    {
                        context.getCounter(Global_Counters.MIN_GRADE).increment(min_grade*(-1));
                        context.getCounter(Global_Counters.MIN_GRADE).increment(student_grade);
                    }

                    context.write(new Text(columns[0]), new IntWritable(student_grade));
                }
            }
            catch (Exception e) 
            {
                e.printStackTrace();
            }
        }
    }

    /* input:  <student, grade>
     * output: <student, normalized_grade>
     */
    public static class Reduce_Normalize extends Reducer<Text, IntWritable, Text, DoubleWritable> 
    {
        public int max_grade, min_grade;

        protected void setup(Context context) throws IOException, InterruptedException 
        {
            Configuration conf = context.getConfiguration();
            Cluster cluster = new Cluster(conf);
            Job current_job = cluster.getJob(context.getJobID());

            max_grade = Math.toIntExact(current_job.getCounters().findCounter(Global_Counters.MAX_GRADE).getValue());
            min_grade = Math.toIntExact(current_job.getCounters().findCounter(Global_Counters.MIN_GRADE).getValue());
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
            // each reducer instance is run for each student, so there is only one value/grade to access
            int student_grade = values.iterator().next().get();

            Double normalized_grade = (double) (student_grade - min_grade) / (max_grade - min_grade);

            context.write(key, new DoubleWritable(normalized_grade));
        }
    }

    public static void main(String[] args) throws Exception 
    {
        Path input_dir = new Path("grades");
        Path output_dir = new Path("normalized_grades");

        Configuration conf = new Configuration();

        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(output_dir))
            fs.delete(output_dir, true);

        Job normalize_job = Job.getInstance(conf, "Normalize Grades");
        normalize_job.setJarByClass(NormGrades.class);
        normalize_job.setMapperClass(Map_Normalize.class);
        normalize_job.setReducerClass(Reduce_Normalize.class);   
        normalize_job.setMapOutputKeyClass(Text.class);
        normalize_job.setMapOutputValueClass(IntWritable.class);
        normalize_job.setOutputKeyClass(Text.class);
        normalize_job.setOutputValueClass(DoubleWritable.class);
        TextInputFormat.addInputPath(normalize_job, input_dir);
        TextOutputFormat.setOutputPath(normalize_job, output_dir);
        normalize_job.waitForCompletion(true);
    }
}
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
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
javac -classpath "$(yarn classpath)" -d . MaxTF.java
jar -cvf MaxTF.jar -C . .
hadoop jar MaxTF.jar MaxTF
hadoop fs -cat max_tf/part-r-00000
*/

public class MaxTF 
{
    /* input:  <document, contents>
     * output: <(word, document), 1>
     */
    public static class WordCountMap extends Mapper<Object, Text, Text, IntWritable> 
    {
        private String filename = "";
        private Text word_doc_key;
        private final static IntWritable one = new IntWritable(1);

        protected void setup(Context context) throws IOException, InterruptedException 
        {
            // get the name of each scanned file to be used as key for the map output pairs
            InputSplit split = context.getInputSplit();
            Path path = ((FileSplit) split).getPath();
            filename = path.getName();

            word_doc_key = new Text();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            // clean up the text of each document
            String[] words = value.toString()
                                .replaceAll("\\d+", "")             // get rid of numbers...
                                .replaceAll("[^a-zA-Z ]", " ")       // get rid of punctuation...
                                .toLowerCase()                      // turn every character left to lowercase...
                                .trim()                             // trim the spaces before & after the whole string...
                                .replaceAll("\\s+", " ")
                                .split(" ");

            for(String word : words)
            {
                if(word != null && !word.trim().isEmpty())
                {
                    word_doc_key.set(word + "@" + filename);
                    context.write(word_doc_key, one);
                }
            }
        }
    }

    /* input: <(word, document), 1>
     * output: <(word, document), word count>
     */
    public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable word_doc_freq;

        protected void setup(Context context) throws IOException, InterruptedException 
        {
            word_doc_freq = new IntWritable();
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException 
        {
            int sum = 0;

            for(IntWritable value : values)
            {
                sum += value.get();
            }

            word_doc_freq.set(sum);

            context.write(key, word_doc_freq);
        }
    }



    /* input:  <(word, document), word count>
     * output: <document, (word, word count)>
     */
    public static class TFMap extends Mapper<Object, Text, Text, Text> 
    {
        private Text document_key;
        private Text word_and_count_value;

        protected void setup(Context context) throws IOException, InterruptedException 
        {
            document_key = new Text();
            word_and_count_value = new Text();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String[] line = value.toString().split("\t");

            String[] split_key = line[0].split("@");

            document_key.set(split_key[1]);
            word_and_count_value.set(split_key[0] + "_" + line[1]);

            context.write(document_key, word_and_count_value);
        }
    }

    /* input: <document, (word, word count)>
     * output: <(word, document), term frequency>
     */
    public static class TFReduce extends Reducer<Text, Text, Text, DoubleWritable>
    {
        private Text word_doc_key;
        private DoubleWritable tf;

        protected void setup(Context context) throws IOException, InterruptedException 
        {
            word_doc_key = new Text();
            tf = new DoubleWritable();
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            // map of the document's terms with their occurences
            HashMap<String, Integer> terms = new HashMap<String, Integer>();

            for(Text value : values)
            {
                String[] split_value = value.toString().split("_");

                terms.put(split_value[0], Integer.parseInt(split_value[1]));
            }

            for(Map.Entry<String,Integer> entry : terms.entrySet()) 
            {
                word_doc_key.set(entry.getKey() + "@" + key.toString());
                tf.set((double) entry.getValue() / terms.size());

                context.write(word_doc_key, tf);
            }
        }
    }



    /* input:  <(word, document), TF>
     * output: <word, (document, TF)>
     */
    public static class MaxTFMap extends Mapper<Object, Text, Text, Text> 
    {
        private Text word_key;
        private Text doc_and_tf_value;

        protected void setup(Context context) throws IOException, InterruptedException 
        {
            word_key = new Text();
            doc_and_tf_value = new Text();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
        {
            String[] line = value.toString().split("\t");

            String[] split_key = line[0].split("@");

            word_key.set(split_key[0]);
            doc_and_tf_value.set(split_key[1] + "_" + line[1]);

            context.write(word_key, doc_and_tf_value);
        }
    }

    /* input: <word, (document, TF)>
     * output: <word, (doc count, max TF doc)>
     */
    public static class MaxTFReduce extends Reducer<Text, Text, Text, Text>
    {
        private Text doc_count_and_max_tf_doc;

        protected void setup(Context context) throws IOException, InterruptedException 
        {
            doc_count_and_max_tf_doc = new Text();
        }

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
        {
            int num_of_docs = 0;
            double max_tf = 0.0;
            String max_tf_doc = "";

            for(Text value : values)
            {
                String[] split_value = value.toString().split("_");

                if(Double.parseDouble(split_value[1]) > max_tf)
                {
                    max_tf = Double.parseDouble(split_value[1]);
                    max_tf_doc = split_value[0];
                }

                num_of_docs++;
            }

            doc_count_and_max_tf_doc.set(num_of_docs + ", " + max_tf_doc);

            context.write(key, doc_count_and_max_tf_doc);
        }
    }


    public static void main(String[] args) throws Exception
    {
        Path input_dir = new Path("metamorphosis");
        Path word_freq_dir = new Path("word_freq");
        Path tf_dir = new Path("tf");
        Path max_tf_dir = new Path("max_tf");

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(word_freq_dir))
            fs.delete(word_freq_dir, true);
        if(fs.exists(tf_dir))
            fs.delete(tf_dir, true);
        if(fs.exists(max_tf_dir))
            fs.delete(max_tf_dir, true);

        Job wordfreq_job = Job.getInstance(conf, "Word Count");
        wordfreq_job.setJarByClass(MaxTF.class);
        wordfreq_job.setMapperClass(WordCountMap.class);
        wordfreq_job.setCombinerClass(WordCountReduce.class);
        wordfreq_job.setReducerClass(WordCountReduce.class);    
        wordfreq_job.setMapOutputKeyClass(Text.class);
        wordfreq_job.setMapOutputValueClass(IntWritable.class);
        wordfreq_job.setOutputKeyClass(Text.class);
        wordfreq_job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(wordfreq_job, input_dir);
        FileOutputFormat.setOutputPath(wordfreq_job, word_freq_dir);
        wordfreq_job.waitForCompletion(true);

        Job tf_job = Job.getInstance(conf, "Term Frequency");
        tf_job.setJarByClass(MaxTF.class);
        tf_job.setMapperClass(TFMap.class);
        tf_job.setReducerClass(TFReduce.class);    
        tf_job.setMapOutputKeyClass(Text.class);
        tf_job.setMapOutputValueClass(Text.class);
        tf_job.setOutputKeyClass(Text.class);
        tf_job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(tf_job, word_freq_dir);
        FileOutputFormat.setOutputPath(tf_job, tf_dir);
        tf_job.waitForCompletion(true);

        Job max_tf_job = Job.getInstance(conf, "Maximum Term Frequency");
        max_tf_job.setJarByClass(MaxTF.class);
        max_tf_job.setMapperClass(MaxTFMap.class);
        max_tf_job.setReducerClass(MaxTFReduce.class);    
        max_tf_job.setMapOutputKeyClass(Text.class);
        max_tf_job.setMapOutputValueClass(Text.class);
        max_tf_job.setOutputKeyClass(Text.class);
        max_tf_job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(max_tf_job, tf_dir);
        FileOutputFormat.setOutputPath(max_tf_job, max_tf_dir);
        max_tf_job.waitForCompletion(true);
    }
}
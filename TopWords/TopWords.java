import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/*
Execution Guide:
javac -classpath "$(yarn classpath)" -d . TopWords.java
jar -cvf TopWords.jar -C . .
hadoop jar TopWords.jar TopWords
hadoop fs -cat topwords/part-r-00000
*/

public class TopWords
{
    /* input:  <document, contents>
     * output: <word, 1>
     */
    public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private final static IntWritable one = new IntWritable(1);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            // clean up the document text and split the words into an array
            String[] words = value.toString()
                                .replaceAll("\\d+", "")           // get rid of numbers...
                                .replaceAll("[^a-zA-Z ]", " ")    // get rid of punctuation...
                                .toLowerCase()                    // turn every letter to lowercase...
                                .trim()                           // trim the spaces...
                                .replaceAll("\\s+", " ")
                                .split(" ");

            // write every word as key with `1` as value that indicates that the word is
            // found at least 1 time inside the input text
            for(String word : words)
                context.write(new Text(word), one);
        }
    }
    
    /* input: <word, 1>
     * output: <word, wordcount>
     */
    public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>
    {
        private IntWritable wordcount = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException
        {
            int word_cnt = 0;

            for(IntWritable value : values)
                word_cnt += value.get();

            wordcount.set(word_cnt);

            context.write(key, wordcount);
        }
    }



    /* input:  <word, wordcount>
     * output: <word, wordcount> (with the local topN words)
     */
    public static class TopNMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private int n;  // the N of TopN
        private TreeMap<Integer, String> word_list; // local list with words sorted by their frequency

        public void setup(Context context)
        {
            n = Integer.parseInt(context.getConfiguration().get("N"));  // get N
            word_list = new TreeMap<Integer, String>();
        }

        public void map(Object key, Text value, Context context)
        {
            String[] line = value.toString().split("\t");   // split the word and the wordcount

            // put the wordcount as key and the word as value in the word list
            // so the words can be sorted by their wordcounts
            word_list.put(Integer.valueOf(line[1]), line[0]);

            // if the local word list is populated with more than N elements
            // remove the first (aka remove the word with the smallest wordcount)
            if (word_list.size() > n)
                word_list.remove(word_list.firstKey());
        }

        public void cleanup(Context context) throws IOException, InterruptedException
        {
            // write the topN local words before continuing to TopNReducer
            // with each word as key and its wordcount as value
            for (Map.Entry<Integer, String> entry : word_list.entrySet())
            {
                context.write(new Text(entry.getValue()), new IntWritable(entry.getKey()));
            }
        }
    }

    /* input:  <word, wordcount> (with the local topN words)
     * output: <wordcount, word> (with the global topN words)
     */
    public static class TopNReducer extends Reducer<Text, IntWritable, IntWritable, Text>
    {
        private int n;  // the N of TopN
        private TreeMap<Integer, String> word_list; //  list with words globally sorted by their frequency

        public void setup(Context context)
        {
            n = Integer.parseInt(context.getConfiguration().get("N"));  // get N
            word_list = new TreeMap<Integer, String>();
        }

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
        {
            int wordcount = 0;

            // get the one and only value (aka the wordcount) for each word
            for(IntWritable value : values)
                wordcount = value.get();

            // put the wordcount as key and the word as value in the word list
            // so the words can be sorted by their wordcounts
            word_list.put(wordcount, key.toString());

            // if the global word list is populated with more than N elements
            // remove the first (aka remove the word with the smallest wordcount)
            if (word_list.size() > n)
                word_list.remove(word_list.firstKey());
        }

        public void cleanup(Context context) throws IOException, InterruptedException
        {
            // write the topN global words with each word as key and its wordcount as value
            // so the output will be sorted by the wordcount
            for (Map.Entry<Integer, String> entry : word_list.entrySet())
            {
                context.write(new IntWritable(entry.getKey()), new Text(entry.getValue()));
            }
        }
    }


    public static void main(String[] args) throws Exception
    {
        Path input_dir = new Path("metamorphosis");
        Path wordcount_dir = new Path("wordcount");
        Path output_dir = new Path("topwords");

        Configuration conf = new Configuration();

        conf.set("N", "10"); // set the N as a "public" value in the current Configuration

        // if the in-between and output directories exists, delete them
        FileSystem fs = FileSystem.get(conf);
        if(fs.exists(wordcount_dir))
            fs.delete(wordcount_dir, true);
        if(fs.exists(output_dir))
            fs.delete(output_dir, true);

        Job wc_job = Job.getInstance(conf, "WordCount");
        wc_job.setJarByClass(TopWords.class);
        wc_job.setMapperClass(WordCountMapper.class);
        wc_job.setCombinerClass(WordCountReducer.class);
        wc_job.setReducerClass(WordCountReducer.class);
        wc_job.setMapOutputKeyClass(Text.class);
        wc_job.setMapOutputValueClass(IntWritable.class);
        wc_job.setOutputKeyClass(Text.class);
        wc_job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(wc_job, input_dir);
        FileOutputFormat.setOutputPath(wc_job, wordcount_dir);
        wc_job.waitForCompletion(true);
        
        Job topn_job = Job.getInstance(conf, "TopN");
        topn_job.setJarByClass(TopWords.class);
        topn_job.setMapperClass(TopNMapper.class);
        topn_job.setReducerClass(TopNReducer.class);
        topn_job.setMapOutputKeyClass(Text.class);
        topn_job.setMapOutputValueClass(IntWritable.class);
        topn_job.setOutputKeyClass(IntWritable.class);
        topn_job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(topn_job, wordcount_dir);
        FileOutputFormat.setOutputPath(topn_job, output_dir);
        topn_job.waitForCompletion(true);
    }
}

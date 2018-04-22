import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import java.nio.ByteBuffer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Main {

    // Custom counter for doc count
    public enum COUNTERS {
        DOCS
    };

    // Handles intermediate sorting between mapper and partitioner
    public static class DocSorter extends WritableComparator {
        protected DocSorter() {
            super(Text.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            String key1 = w1.toString();
            String key2 = w2.toString();
            return -1 * key1.compareTo(key2);
        }
    }

    public static class DocPartitioner extends Partitioner < Text, IntWritable> {

        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String naturalKey = key.toString();
            String[] tokenArray = naturalKey.split("_");
            naturalKey = tokenArray[0];

            return (naturalKey.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    public static class DocPartitioner2 extends Partitioner < Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) {
            String naturalKey = key.toString();
            String[] tokenArray = naturalKey.split("_");
            naturalKey = tokenArray[0];

            return (naturalKey.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }

    public static class DocPartitioner3 extends Partitioner < Text, Text> {

        @Override
        public int getPartition(Text key, Text value, int numReduceTasks) {
            String naturalKey = key.toString();
            
            return (naturalKey.hashCode() & Integer.MAX_VALUE) % numReduceTasks;
        }
    }
    // START FIRST MAP REDUCE TASK
    // Finds term frequency per document    

    // Clean up each token to match requirements
    public static class TokenizerMapper
    extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (!value.toString().isEmpty()) {
                String[] tokens = value.toString().split("<====>");
                if(!(tokens.length < 3)) {
                    StringTokenizer itr = new StringTokenizer(tokens[2]);
                    context.getCounter(COUNTERS.DOCS).increment(1); // Counts documents
                    while (itr.hasMoreTokens()) {
                        word.set(itr.nextToken());
                        String clean_word = word.toString();

                        // Drop all non-alphanumeric chars and switch to lower
                        clean_word = clean_word.replaceAll("[^A-Za-z0-9]", "");
                        clean_word = clean_word.toLowerCase();
                        clean_word = tokens[1] + "_" + clean_word;

                        // Send data up
                        if(clean_word.length() > 0 && clean_word.split("_").length > 1){
                            Text final_word = new Text(clean_word);
                            context.write(final_word, one);
                        }else{
                            continue;
                        }
                    }
                }
            }
        }
    }

    // Reduce to get counts
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,
        Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            
            context.write(key, result);
        }
    }

    // START SECOND MAP REDUCE CYCLE
    // Right now we're working with <docID_unigram,frequency>
    // Wanna come out of this with the TF per word

    // Switchs the placement of the unigram to the value, docID alone becomes key
    public static class SwapWord extends Mapper<LongWritable, Text, Text, Text>{
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (!value.toString().isEmpty()) {
                //String key_string = key.toString();
                // Here the value has everything we want and the key is a garbage number
                
                // So split the value up
                String[] value_split = value.toString().split("_");
                
                // Grab the doc ID for the key
                Text final_key = new Text(value_split[0]);

                // Drop the tab and replace with our preferred delimiter
                String val_string = value_split[1].replace("\t", "_"); 
                Text final_value = new Text(val_string); 

                // And it's done, send it off
                context.write(final_key, final_value);
            }
        }
    }

    // Compute all TF values
    public static class TFVal extends Reducer<Text,Text,Text,Text> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<Text> values,
        Context context
        ) throws IOException, InterruptedException {
            int maxF = 0;
            ArrayList<Text> buffer = new ArrayList<Text>();

            // Start out by finding max freq
            for (Text val : values) {
                // Cache all values for second loop
                buffer.add(new Text(val));

                // First just get out number out
                String tmp = val.toString();
                String[] tokens = tmp.split("_");
                int freq = Integer.parseInt(tokens[1]);
                if(freq > maxF) {
                    maxF = freq;
                }
            }

            // Okay, we've got it! Now compute normalized TF for each word
            //for (Text val : values) {
            for (Text val : buffer) {
                // Deconstruct string
                String tmp = val.toString();
                String[] tokens = tmp.split("_");
                int freq = Integer.parseInt(tokens[1]);

                // Calculate TF
                double tf = 0.5 + (0.5*((double)freq/(double)maxF));

                // Reconstruct string
                String value = tmp + "_" + Double.toString(tf);
                //String value = Double.toString(tf); // DEBUG
                //value = "maxF:" + Integer.toString(maxF) + " freq: " + Integer.toString(freq) + " tf:" + Double.toString(tf); // DEBUG
                Text final_val = new Text(value);

                // Send result 
                context.write(key, final_val);
                
            }
        }
    }

    // START THIRD MAP REDUCE CYCLE
    // Right now we're working with <docID, unigram_frequency_tf>
    // After reduce we want <unigram, docID_TF_n>

    // Swap so unigram is key, drop frequency
    public static class SwapUnigramDoc extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (!value.toString().isEmpty()) {

                // Key gets caught in value, so ditch it here
                String val_string = value.toString();
                String[] val_tokens = val_string.split("\t");
                val_string = val_tokens[1]; // unigram_freq_TF
                String doc_id = val_tokens[0];

                // Extract the unigram and make that the key
                String[] sub_val_tokens = val_string.split("_");
                String key_string = sub_val_tokens[0];
                Text final_key = new Text(key_string);

                // Build the final value (doc_id_tf)
                String close_val = doc_id + "_" + sub_val_tokens[2];
                Text final_val = new Text(close_val);
                
                // And it's done, send it off
                context.write(final_key, final_val);
            }
        }
    }

    // Compute all TF values
    public static class GetN extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
        Context context
        ) throws IOException, InterruptedException {
            ArrayList<Text> buffer = new ArrayList<Text>();
            int count = 0;

            // Finds total count of words in whole corpus
            for (Text val : values) {
                buffer.add(new Text(val));
                count++;
            }

            // Adds this to value, outputs result properly
            for (Text val : buffer) {
                // Each val starts as docID_TF, should have n appended to end
                String tmp_string = val.toString();
                tmp_string = tmp_string + "_" + Integer.toString(count); 
                Text final_val = new Text(tmp_string);
                context.write(key, final_val);
            }

        }
    }

    // START FOURTH MAP REDUCE CYCLE
    // Right now we're working with <docID, unigram_tf_n>
    // After reduce we want <docID, docID_TF_TF-IDF>

    // Swap so unigram is key, drop frequency
    public static class ComputeIDF extends Mapper<LongWritable, Text, Text, Text>{
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (!value.toString().isEmpty()) {
                // Get count from driver
                long random_long = 0;
                long count_val = context.getConfiguration().getLong("count", random_long);
                double doc_count = (double) count_val;

                // Split incoming data key and value
                String input_string = value.toString();
                String[] input_tokens = input_string.split("\t");
                String val_string = input_tokens[1]; // docID_TF_n 
                String unigram = input_tokens[0];

                // Split up value data
                String[] val_tokens = val_string.split("_");
                double idf = Math.log10(doc_count/Double.parseDouble(val_tokens[2]));
                double tf = Double.parseDouble(val_tokens[1]);
                double tf_idf = tf * idf;
                String output_value = unigram + "_" + val_tokens[1] + "_" + Double.toString(tf_idf);
                
                // Create final key and value
                Text final_key = new Text(val_tokens[0]);
                Text final_val = new Text(output_value);
                
                context.write(final_key, final_val);
            }
        }
    }

    // Compute all TF values
    public static class ReduceIDF extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
        Context context
        ) throws IOException, InterruptedException {

            for(Text val : values) {
                context.write(key, val);
            }
        }
    }
    
    // START FIFTH MAP REDUCE CYCLE

    // Mapper to handle reading Phase 4 output
    // Outputs <docID, unigram_tf-idf>
    public static class RankingMapper1
    extends Mapper<LongWritable, Text, Text, Text>{
        private Text word = new Text();
        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (!value.toString().isEmpty()) {
                // Extract correct key
                String[] kv_tokens = value.toString().split("\t");
                Text final_key = new Text(kv_tokens[0]);

                // Get each word and tf-idf, set final value
                String[] val_tokens = kv_tokens[1].split("_"); // <unigram_tf_tf-idf>
                String tmp_val = val_tokens[0] + "_" + val_tokens[2];
                Text final_val = new Text(tmp_val);

                context.write(final_key, final_val);
            }
        }
    }

    // Mapper to handle reading the full dataset
    // Outputs <docID, [list of sentences]>
    public static class RankingMapper2
    extends Mapper<Object, Text, Text, Text>{
        private Text word = new Text();
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (!value.toString().isEmpty()) {
                String[] tokens = value.toString().split("<====>");
                if(!(tokens.length < 3)) {
                    //StringTokenizer itr = new StringTokenizer(tokens[2]);
                    String currText = tokens[2];
                    String docID = tokens[1];
                    Text final_key = new Text(docID);
                    String[] textTokens = currText.split("\\.");
                    for (String currSentence : textTokens) {
                        // Drop all non-alphanumeric chars and switch to lower
                        currSentence = currSentence.replaceAll("[^A-Za-z0-9\\s]", "");
                        currSentence = currSentence.toLowerCase();

                        // Send data up
                        if(currSentence.length() > 0){
                            Text final_word = new Text(currSentence);
                            context.write(final_key, final_word);
                        }else{
                            continue;
                        }
                    }
                }
            }
        }
    }

    // Reducer to join and find best sentences for each docID
    // Outputs <docID, [top 3 sentences]>
    public static class RankingReducer extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
        Context context
        ) throws IOException, InterruptedException {
            // These store everything our data as it's organized
            Map<String, Double> wordValues = new HashMap<String, Double>();
            Map<String, Double> sentenceValues = new HashMap<String, Double>();
            ArrayList<String> orderedSentences = new ArrayList<String>();

            // This is used to find data values 
            String pattern = "_";
            Pattern data = Pattern.compile(pattern);

            // First separate the sentences and tf-idf values
            for (Text val : values) {
                String stringVal = val.toString();
                Matcher m = data.matcher(stringVal);
                if (m.find()) {
                    // We're dealing we data
                    String[] wordTokens = stringVal.split("_");
                    wordValues.put(wordTokens[0], Double.parseDouble(wordTokens[1]));
                }else {
                    // We're dealing with a sentence
                    orderedSentences.add(stringVal);
                    sentenceValues.put(stringVal, null);
                }
            }

            // Now determine each sentence score (sum of 5 highest TF-IDF values)            
            for (String sentence : sentenceValues.keySet()) {
                ArrayList<Double> wordScores = new ArrayList<Double>();
                StringTokenizer itr = new StringTokenizer(sentence);

                // Store the value of each word in the sentence
                while (itr.hasMoreTokens()) {
                    String word = itr.nextToken();
                    
                    double tmpVal = wordValues.get(word);
                    wordScores.add(tmpVal);
                }

                // Sort the word values and add them
                double totalScore = 0;
                if (wordScores.size() < 6) {
                    for (double i : wordScores) {
                        totalScore += i; 
                    } 
                }else {
                    Collections.sort(wordScores, Collections.reverseOrder());
                    for (int i = 0; i < 5; i++) {
                        totalScore += wordScores.remove(0);    
                    }
                }

                // Now we associate the score with the sentence
                sentenceValues.put(sentence, totalScore);
            }

            // Now we need the top 3 sentences to be returned in order
            List<Double> tmpList = new ArrayList<Double>(sentenceValues.values());
            Collections.sort(tmpList, Collections.reverseOrder());
            boolean one = false;
            boolean two = false;
            boolean three = false;
            if (tmpList.size() > 2) {
                List<Double> top3 = tmpList.subList(0, 3);
                //for (String s : sentenceValues.keySet()) {
                for (String s : orderedSentences) {
                    if (top3.get(0) == sentenceValues.get(s) && !one) {
                        context.write(key, new Text(s));
                        one = true;
                    }
                    else if (top3.get(1) == sentenceValues.get(s) && !two) {
                        context.write(key, new Text(s));
                        two = true;
                    }
                    else if (top3.get(2) == sentenceValues.get(s) && !three) {
                        context.write(key, new Text(s));
                        three = true;
                    }

                    if ( one && two && three ) {
                        break;
                    }

                } 

            }else {
                // Idk, skipping for now, need clarification
            }
        }
    }
    
    // MAIN STUFF
    // Here we set up all the job contingencies and shit
    
    public static void main(String[] args) throws Exception {
        // JOB SETUP
        // Only for phases 1-3, then a break to get some values and resume

        Configuration conf = new Configuration();
        //JobControl jobControl = new JobControl("jobCahin");
        Job job1 = Job.getInstance(conf, "Raw Frequency");

        // Job1 setup
        job1.setJarByClass(Main.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setPartitionerClass(DocPartitioner.class);
        job1.setCombinerClass(IntSumReducer.class);
        job1.setReducerClass(IntSumReducer.class);
        job1.setNumReduceTasks(5);
        // Set expected output values
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        
        // Set up job control on job1
        ControlledJob controlledJob1 = new ControlledJob(conf);
        controlledJob1.setJob(job1);
        //jobControl.addJob(controlledJob1);

        // Set up job2 conf
        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf, "Compute TFs");
        job2.setJarByClass(Main.class);

        // Set up job control on job2
        ControlledJob controlledJob2 = new ControlledJob(conf2);
        controlledJob2.setJob(job2);
        controlledJob2.addDependingJob(controlledJob1);
        //jobControl.addJob(controlledJob2);
        
        // Job2 setup
        job2.setMapperClass(SwapWord.class);
        job2.setReducerClass(TFVal.class);
        job2.setPartitionerClass(DocPartitioner2.class);
        job2.setNumReduceTasks(5);
        // Set expected output values
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        // Set up job3 conf
        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf, "get combined frequency");
        job3.setJarByClass(Main.class);

        // Set up job control on job3
        ControlledJob controlledJob3 = new ControlledJob(conf3);
        controlledJob3.setJob(job3);
        controlledJob3.addDependingJob(controlledJob2);
        //jobControl.addJob(controlledJob3);
        
        // Job3 setup
        job3.setMapperClass(SwapUnigramDoc.class);
        job3.setReducerClass(GetN.class);
        job3.setPartitionerClass(DocPartitioner3.class);
        job3.setNumReduceTasks(5);
        // Set expected output values
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(Text.class);

        // Paths and exit
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/tmp_1"));
        FileInputFormat.addInputPath(job2, new Path(args[1] + "/tmp_1"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/tmp_2"));
        FileInputFormat.addInputPath(job3, new Path(args[1] + "/tmp_2"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1] + "/tmp_3"));
        
        // PHASE 1-3 EXECUTION
        // Kick off actual execution
        System.out.println("Status: Waiting for job completion");
        job1.waitForCompletion(true);
        JobID count_job = job1.getJobID();
        long doc_count = job1.getCounters().findCounter(COUNTERS.DOCS).getValue();
        System.out.println("Status: Job 1 completed");
        job2.waitForCompletion(true);
        System.out.println("Status: Job 2 completed");
        job3.waitForCompletion(true);
        System.out.println("Status: Job 3 completed");

        // Set up job4 conf
        Configuration conf4 = new Configuration();
        conf4.setLong("count", doc_count);
        Job job4 = Job.getInstance(conf4, "Compute IDF");
        job4.setJarByClass(Main.class);

        // Job4 setup
        job4.setMapperClass(ComputeIDF.class);
        job4.setReducerClass(ReduceIDF.class);
        job4.setPartitionerClass(DocPartitioner3.class);
        job4.setNumReduceTasks(5);
        // Set expected output values
        job4.setMapOutputKeyClass(Text.class);
        job4.setMapOutputValueClass(Text.class);
        job4.setOutputKeyClass(Text.class);
        job4.setOutputValueClass(Text.class);

        // Paths for job 4 and after
        FileInputFormat.addInputPath(job4, new Path(args[1] + "/tmp_3"));
        FileOutputFormat.setOutputPath(job4, new Path(args[1] + "/tmp_4"));

        // Launch phase 4, gets IDF
        job4.waitForCompletion(true);
        System.out.println("Status: Job 4 completed");
        
        // Set up job5 conf
        Configuration conf5 = new Configuration();
        Job job5 = Job.getInstance(conf5, "Rank Sentences");
        job5.setJarByClass(Main.class);

        // Job5 setup
        //job5.setMapperClass(RankingMapper1.class);
        job5.setReducerClass(RankingReducer.class);
        job5.setPartitionerClass(DocPartitioner3.class);
        job5.setNumReduceTasks(5);
        //job5.setNumReduceTasks(1);
        // Set expected output values
        job5.setMapOutputKeyClass(Text.class);
        job5.setMapOutputValueClass(Text.class);
        job5.setOutputKeyClass(Text.class);
        job5.setOutputValueClass(Text.class);

        // Paths for job 4 and after
        MultipleInputs.addInputPath(job5, new Path(args[1] + "/tmp_4"), TextInputFormat.class, RankingMapper1.class);
        MultipleInputs.addInputPath(job5, new Path(args[0]), TextInputFormat.class, RankingMapper2.class);
        //FileInputFormat.addInputPath(job5, new Path(args[1] + "/tmp_4"));
        FileOutputFormat.setOutputPath(job5, new Path(args[1] + "/final"));
    
        job5.waitForCompletion(true);
        System.out.println("Status: Job 5 completed");
        
        System.exit(0);
        //System.exit(job4.waitForCompletion(true) ? 0 : 1);

    }
}

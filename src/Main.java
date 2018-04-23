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

    // Basic sorter for sorting by value 
    // Assumes it's sorting by a value added to the end of the key like <key>_5
    public static class ValueSorter extends WritableComparator {
        protected ValueSorter() {
            super(Text.class, true);
        }

        @SuppressWarnings("rawtypes")
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            //String key1 = w1.toString().split("_")[1];
            //String key2 = w2.toString().split("_")[1];
            int key1 = Integer.parseInt(w1.toString().split("_")[1]);
            int key2 = Integer.parseInt(w2.toString().split("_")[1]);
            if ( key1 == key2 ) {
                return 0;
            } else {
                return -1 * (key1 - key2);
            }

            //return -1 * key1.compareTo(key2);
        }
    }

    // START FIRST MAP REDUCE TASK
    // Finds number of times each player hit a foul

    // Map a name to each foul
    public static class FoulMapper
    extends Mapper<Object, Text, Text, IntWritable>{
        private final static IntWritable one = new IntWritable(1);
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (!value.toString().isEmpty()) {
                String[] tokens = value.toString().split(":"); // Splits name and play?
                
                // Split up play 
                String[] playComponents = tokens[1].split("/");

                // Iterate over play, find what we want
                for (String play : playComponents) {
                    if (play.equals("F")) {
                        Text final_key = new Text(tokens[0]);
                        context.write(final_key, one); // Map player code to 1 (like wordcount)
                    }
                }

            }
        }
    }

    // Reduce to get counts
    public static class FoulReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values,
        Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            
            // Create compound key, for 2nd stage sorting
            Text final_key = new Text(key.toString() + "_" + result.toString());
            
            //context.write(key, result);
            context.write(final_key, result);
        }
    }

    // START SECOND MAP REDUCE TASK
    // Sorts data from first MapReduce

    // Identity mapper (no functional change)
    public static class SortMapper
    extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (!value.toString().isEmpty()) {
                String[] tokens = value.toString().split("\t"); // Splits key and value

                Text final_key = new Text(tokens[0]);
                Text final_val = new Text(tokens[1]);
                context.write(final_key, final_val); 

            }
        }
    }

    // Now sorted so string unwanted data before writing
    public static class SortReducer extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,
        Context context
        ) throws IOException, InterruptedException {
            // Strip unneeded value from key now that sort is complete 
            String[] tokens = key.toString().split("_");
            Text final_key = new Text(tokens[0]);

            // Just to be safe we can do this
            Text final_val = new Text(tokens[1]);
            
            context.write(final_key, final_val);
        }
    }
    // MAIN STUFF
    public static void main(String[] args) throws Exception {
        // Create job1
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Finding number of foul balls per player");

        // job1 setup
        job1.setJarByClass(Main.class);
        job1.setMapperClass(FoulMapper.class);
        job1.setCombinerClass(FoulReducer.class);
        job1.setReducerClass(FoulReducer.class);
        job1.setNumReduceTasks(1);
        // Set expected output values
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);

        // Paths and exit
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/tmp_1"));
        
        // Kick off actual execution
        System.out.println("Status: Waiting for job1 completion...");
        job1.waitForCompletion(true);
        System.out.println("Status: job1 completed");

        // Create job2
        Job job2 = Job.getInstance(conf, "Sorting list");
        
        // job2 setup
        job2.setJarByClass(Main.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        job2.setSortComparatorClass(ValueSorter.class);
        job2.setNumReduceTasks(1);
        // Set expected output values
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        
        // Paths and exit
        FileInputFormat.addInputPath(job2, new Path(args[1] + "/tmp_1"));
        FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/results"));
        
        // Execute job2
        System.out.println("Status: Waiting for job2 completion...");
        job2.waitForCompletion(true);
        System.out.println("Status: job2 completed");

        System.exit(0);
    }
}

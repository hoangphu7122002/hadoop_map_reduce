//package hadoop;

import java.util.*;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class join_v0 {
    public static class joinMapper1 
        extends Mapper<Object,Text,Text,IntWritable> {
            //private final static DoubleWritable one = new DoubleWritable(1);
            private Text word = new Text();
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                StringTokenizer itr = new StringTokenizer(value.toString(),"\t");
                String token = null;
                String id = itr.nextToken();
		int count = 0;
                while (itr.hasMoreTokens()) {
                    token = itr.nextToken();
		    if (itr.hasMoreTokens()) {
			count = 1;
		    }
                    break;
                }
                if (id.contains("user_ptr_id")) return;
		if (id.contains("id")) return;
		if (token.length() < 3) return;
		if (id.contains("\"")) id = id.substring(1,id.length()-1);
		if (token.contains("\"")) token = token.substring(1,token.length()-1);
                int val = Integer.parseInt(token);
                word.set(id);
		if (count == 1) context.write(word,new IntWritable(2 * val));
		else context.write(word,new IntWritable(2 * val + 1));
		//context.collect(word,new IntWritable(2 * val));
            }
        }

    public static class joinReducer
        extends Reducer<Text,IntWritable,Text,Text> {
            // private DoubleWritable result = new DoubleWritable();
            public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int sum_score = 0;
                int cnt = 0;
                int reputation = 0;
                for (IntWritable val: values) {
		  //String cur_val = val.toString();
		  //int val_i = Integer.parseInt(cur_val.substring(1));
		  int val_i = val.get();
		  if (val_i % 2 == 0) {
		      reputation = val_i / 2;
		      System.out.println("r " + reputation);
		  }
		  if (val_i % 2 == 1) {
		      cnt += 1;
		      sum_score += val_i / 2;
		      System.out.println(sum_score + " " + cnt);
		  }
                }

		if (cnt == 0) cnt = 1;
                //MapWritable m = new MapWritable();
                Double res = (Double)((sum_score * 1.0) / cnt);
                //m.put(new Text("reputation"), new LongWritable(reputation));
                //m.put(new Text("score"), new DoubleWritable(res));
                //m.put(new Text("count"), new IntWritable(cnt));
                context.write(key,new Text(Integer.toString(reputation)+ " " + Double.toString(res)));
            }
        }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"join_v0");
        job.setJarByClass(join_v0.class);
        //job.setMapperClass(joinMapper.class);
        //job.setCombinerClass(joinCombiner.class);
        //job.setReducerClass(joinReducer.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
	//job.setNumReduceTasks(1);
        // FileInputFormat.addInputPath(job,new Path(args[0]));
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,joinMapper1.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,joinMapper1.class);
	//job.setCombinerClass(joinCombiner.class);
	job.setNumReduceTasks(1);
	job.setReducerClass(joinReducer.class);
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

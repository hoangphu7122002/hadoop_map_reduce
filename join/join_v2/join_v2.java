//package hadoop;

import java.util.*;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class join_v2 {
    public static class joinMapper1 
        extends Mapper<Object,Text,Text,MapWritable> {
            //private final static DoubleWritable one = new DoubleWritable(1);
            private Text word = new Text();
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                StringTokenizer itr = new StringTokenizer(value.toString(),"\t");
                String token = null;
                String id = itr.nextToken();
                while (itr.hasMoreTokens()) {
                    token = itr.nextToken();
                    break;
                }
                if (id.contains("user_ptr_id")) return;
		if (id.contains("\"")) id = id.substring(1,id.length()-1);
		if (token.contains("\"")) token = token.substring(1,token.length()-1);
                int val = Integer.parseInt(token);
                MapWritable m = new MapWritable();
                m.put(new Text("reputation"), new IntWritable(val));
		m.put(new Text("score"),new IntWritable(0));
		m.put(new Text("count"),new IntWritable(0));
                word.set(id);
                context.write(word,m);
            }
        }
    
    public static class joinMapper2
        extends Mapper<Object,Text,Text,MapWritable> {
            //private final static DoubleWritable one = new DoubleWritable(1);
            private Text word = new Text();
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                StringTokenizer itr = new StringTokenizer(value.toString(),"\t");
                String token = null;
                String id_save = null;
                String id = itr.nextToken();
                //int count = 1;
                while (itr.hasMoreTokens()) {
                    token = itr.nextToken();
                }
                if (id.contains("id")) return;
		//System.out.println(token + " " + token.length());
		int len = token.length();
		if (len < 3) return;
		if (token.contains("\"")) token = token.substring(1,token.length() - 1);
		if (id.contains("\"")) id = id.substring(1,id.length()-1);
		int score = Integer.parseInt(token);
                MapWritable m = new MapWritable();
		m.put(new Text("reputation"), new IntWritable(0));
                m.put(new Text("score"), new IntWritable(score));
                m.put(new Text("count"), new IntWritable(1));
                word.set(id);
                context.write(word,m);
            }
        }

    public static class joinCombiner
        extends Reducer<Text,MapWritable,Text,MapWritable> {
        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            int sum_score = 0;
            int cnt = 0;
            int reputation = 0;
            for (MapWritable val: values) {
                if (val.containsKey(new Text("reputation"))) {
                    reputation = Integer.valueOf(val.get(new Text("reputation")).toString());
		    System.out.println("reputation: " + reputation);
                }
		if (val.containsKey(new Text("score"))) {
                   sum_score += Integer.valueOf(val.get(new Text("score")).toString());
                   cnt += Integer.valueOf(val.get(new Text("count")).toString());
		   System.out.println("===========");
		   System.out.println(sum_score);
		   System.out.println(cnt);
		   System.out.println("===========");
		}
            }
            
            MapWritable m = new MapWritable();
            m.put(new Text("score"), new IntWritable(sum_score));
            m.put(new Text("count"), new IntWritable(cnt));
            m.put(new Text("reputation"), new IntWritable(reputation));
            //m.put(new Text("count"), new IntWritable(cnt));
            context.write(key,m);
        }
    }

    public static class joinReducer
        extends Reducer<Text,MapWritable,Text,MapWritable> {
            // private DoubleWritable result = new DoubleWritable();
            public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
                int sum_score = 0;
                int cnt = 0;
                int reputation = 0;
                for (MapWritable val: values) {
                    if (val.containsKey(new Text("reputation"))) {
                        reputation = Integer.valueOf(val.get(new Text("reputation")).toString());
                    }
		    if (val.containsKey(new Text("score"))) {
	                sum_score += Integer.valueOf(val.get(new Text("score")).toString());
        	        cnt += Integer.valueOf(val.get(new Text("count")).toString());
			//System.out.println("!!!!!!!!!!!");
			//System.out.println(sum_score);
			//System.out.println(cnt);
			//System.out.println("!!!!!!!!!!!");
		    }
                }
                
                MapWritable m = new MapWritable();
                Double res = (Double)((sum_score * 1.0) / cnt);
		m.put(new Text("cnt"), new IntWritable(cnt));
		m.put(new Text("score"),new IntWritable(sum_score));
                m.put(new Text("reputation"), new IntWritable(reputation));
                m.put(new Text("res_score"), new DoubleWritable(res));
                //m.put(new Text("count"), new IntWritable(cnt));
                context.write(key,m);
            }
        }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"join_v2");
        job.setJarByClass(join_v2.class);
        //job.setMapperClass(joinMapper.class);
        //job.setCombinerClass(joinCombiner.class);
        //job.setReducerClass(joinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MapWritable.class);
	//job.setNumReduceTasks(1);
        // FileInputFormat.addInputPath(job,new Path(args[0]));
        MultipleInputs.addInputPath(job,new Path(args[0]),TextInputFormat.class,joinMapper1.class);
        MultipleInputs.addInputPath(job,new Path(args[1]),TextInputFormat.class,joinMapper2.class);
	job.setCombinerClass(joinCombiner.class);
	job.setReducerClass(joinReducer.class);
        FileOutputFormat.setOutputPath(job,new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

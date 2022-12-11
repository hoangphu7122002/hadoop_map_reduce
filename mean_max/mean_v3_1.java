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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class mean_v3 {
    public static class meanMapper 
        extends Mapper<Object,Text,Text,MapWritable> {
            //private final static DoubleWritable one = new DoubleWritable(1);
            private Text word = new Text();
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                StringTokenizer itr = new StringTokenizer(value.toString(),",");
                String token = null;
                String ym = itr.nextToken().substring(2);
                
                while (itr.hasMoreTokens()) {
                    token = itr.nextToken();
                }
                Double val = Double.parseDouble(token);
                MapWritable m = new MapWritable();
                m.put(new Text("sum"), new DoubleWritable(val));
                m.put(new Text("count"), new IntWritable(1));
                word.set(ym);
                context.write(word,m);
            }
        }

    public static class meanReducer 
        extends Reducer<Text,MapWritable,Text,DoubleWritable> {
        @Override
        public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
            Double sum = 0.0;
            int cnt = 0;
            for (MapWritable val: values) {
                sum += Double.valueOf(val.get(new Text("sum")).toString());
                cnt += Integer.valueOf(val.get(new Text("count")).toString());
            }
            
            Double avg = sum / cnt;
            //MapWritable m = new MapWritable();
            //m.put(new Text("avg"), new DoubleWritable(avg));
            //m.put(new Text("count"), new IntWritable(cnt));
            //context.write(key,m);
	    context.write(key,new DoubleWritable(avg));
        }
    }

    public static class meanCombiner
        extends Reducer<Text,MapWritable,Text,MapWritable> {
            // private DoubleWritable result = new DoubleWritable();
            public void reduce(Text key, Iterable<MapWritable> values, Context context) throws IOException, InterruptedException {
                Double sum = 0.0;
                int cnt = 0;
                for (MapWritable val: values) {
                    sum += Double.valueOf(val.get(new Text("sum")).toString());
                    cnt += Integer.valueOf(val.get(new Text("count")).toString());
                }
                MapWritable m = new MapWritable();
                m.put(new Text("sum"), new DoubleWritable(sum));
                m.put(new Text("count"), new IntWritable(cnt));
                // result.set(sum/cnt);
                context.write(key,m);
            }
        }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"mean_v3");
        job.setJarByClass(mean_v3.class);
        job.setMapperClass(meanMapper.class);
        job.setCombinerClass(meanCombiner.class);
        job.setReducerClass(meanReducer.class);
	job.setMapOutputKeyClass(Text.class);
	job.setMapOutputValueClass(MapWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

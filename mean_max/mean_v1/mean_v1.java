//package hadoop;

import java.util.*;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class mean_v1 {
    public static class meanMapper 
        extends Mapper<Object,Text,Text,DoubleWritable> {
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
                word.set(ym);
                context.write(word,new DoubleWritable(val));
            }
        }

    public static class meanReducer
        extends Reducer<Text,DoubleWritable,Text,DoubleWritable> {
            private DoubleWritable result = new DoubleWritable();
            public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
                Double sum = 0.0;
                int cnt = 0;
                for (DoubleWritable val: values) {
                    sum += val.get();
                    cnt += 1;
                }
                result.set(sum/cnt);
                context.write(key,result);
            }
        }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"mean_v1");
        job.setJarByClass(mean_v1.class);
        job.setMapperClass(meanMapper.class);
        //job.setCombinerClass(maxMapper.class);
        job.setReducerClass(meanReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

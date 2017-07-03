package yelpexamples;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q1{

	public static class BusinessMap extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] businessData = value.toString().split("\\^");
			if (businessData.length ==3) {
				if(businessData[1].contains("Palo Alto"))
					context.write(new Text(businessData[1]), new IntWritable(1));
			}		
		}
	}
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable> {
		public void reduce(Text key, Iterable<IntWritable> values,Context context ) throws IOException, InterruptedException {
			int count=0;
			for(@SuppressWarnings("unused") IntWritable t : values){
				count++;
			}
			context.write(key,new IntWritable(count));
		}
	}
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();	  
		Job job = Job.getInstance(conf, "Q1");
		job.setJarByClass(Q1.class);   
		job.setMapperClass(BusinessMap.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}



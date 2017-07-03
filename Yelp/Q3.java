package yelpexamples;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q3{
	public static class BusinessMap extends Mapper<LongWritable, Text, Text, IntWritable>{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] businessData = value.toString().split("\\^");
			if (businessData.length > 2){
				String[] address = businessData[1].split(",");
				context.write(new Text(address[1]),new IntWritable(1));			
			}		
		}
	}	
	public static class BusinessReduce extends Reducer<Text,IntWritable,Text,IntWritable> {

		private Map<String, Integer> countMap = new HashMap<>();
		public void reduce(Text key, Iterable<IntWritable> values,Context context ) throws IOException, InterruptedException {
			int count=0;
			for (@SuppressWarnings("unused") IntWritable val : values) {
				count++;
			}
			countMap.put(key.toString(), count);
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			Set<Entry<String, Integer>> set = countMap.entrySet();
			List<Entry<String, Integer>> list = new ArrayList<Entry<String, Integer>>(set);
			Collections.sort( list, new Comparator<Map.Entry<String, Integer>>(){
				public int compare( Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2 )
				{
					return (o2.getValue()).compareTo( o1.getValue() );
				}
			} );
			int counter = 0;
			for(Map.Entry<String, Integer> entry:list){
				if(counter <10) {
					context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
					counter++;
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Q3");
		job.setJarByClass(Q3.class);
		job.setMapperClass(BusinessMap.class);
		job.setReducerClass(BusinessReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
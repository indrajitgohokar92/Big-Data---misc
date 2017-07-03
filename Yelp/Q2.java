package yelpexamples;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q2{
	public static class BusinessMap extends Mapper<LongWritable, Text, Text, Text>{
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] businessData = value.toString().split("\\^");
			if (businessData.length > 2){
				if(businessData[1].contains("NY")== true){
					if(businessData[2].contains("Restaurants")==true)
						context.write(new Text(businessData[0]),new Text(businessData[1]));
				}			
			}		
		}
	}
	public static class Reduce extends Reducer<Text,Text,Text,Text> {
		private Text result = new Text();
		private Text myKey = new Text();
		public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException {	
			HashMap<String,String> map = new HashMap<String,String>();
			for (Text val : values) {
				if(map.get(key.toString())==null)
				{
					map.put(key.toString(), val.toString());
					result.set(val.toString());
					myKey.set(key.toString());
					context.write(myKey,result);
				}					
			}
		}
	}	

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Q2");
		job.setJarByClass(Q2.class);
		job.setMapperClass(BusinessMap.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
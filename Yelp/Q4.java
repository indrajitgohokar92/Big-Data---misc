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

public class Q4{

	public static class ReviewMap extends Mapper<LongWritable, Text, Text, DoubleWritable>{

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] reviewData = value.toString().split("\\^");
			if (reviewData.length ==4) {
				try {
					double rating = Double.parseDouble(reviewData[3]);
					context.write(new Text(reviewData[2]), new DoubleWritable(rating));
				}
				catch (NumberFormatException e) {
					context.write(new Text(reviewData[2]), new DoubleWritable(0.0));
				}
			}		
		}
	}

	public static class ReviewReduce extends Reducer<Text,DoubleWritable,Text,Text> {

		private Map<String, Double> countMap = new HashMap<>();
		public void reduce(Text key, Iterable<DoubleWritable> values,Context context ) throws IOException, InterruptedException {
			int count=0;
			double sum = 0.0;
			for (DoubleWritable val : values) {
				sum += val.get();
				count++;
			}
			Double avg =  ((double)sum/(double)count);
			countMap.put(key.toString(), avg);
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Set<Entry<String, Double>> set = countMap.entrySet();
			List<Entry<String, Double>> list = new ArrayList<Entry<String, Double>>(set);
			Collections.sort( list, new Comparator<Map.Entry<String, Double>>(){
				public int compare( Map.Entry<String, Double> m1, Map.Entry<String, Double> m2 )
				{
					return (m2.getValue()).compareTo( m1.getValue() );
				}
			} );
			int counter = 0;
			for(Map.Entry<String, Double> entry:list){
				if(counter <10) {
					context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
					counter++;
				}
			}	        
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Q4");
		job.setJarByClass(Q4.class);
		job.setMapperClass(ReviewMap.class);
		job.setReducerClass(ReviewReduce.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setMapOutputKeyClass(Text.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
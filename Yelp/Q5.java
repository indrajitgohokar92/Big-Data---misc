package yelpexamples;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class Q5{

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

		private Map<Text, Double> countMap = new HashMap<>();
		public void reduce(Text key, Iterable<DoubleWritable> values,Context context ) throws IOException, InterruptedException {

			int count=0;
			double sum = 0.0;
			for (DoubleWritable val : values) {
				sum += val.get();
				count++;
			}
			Double avg =  ((double)sum/(double)count);
			countMap.put(new Text(key), avg);
		}
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Map<Text, Double> sortedMap = sortByValues(countMap);
			int counter = 0;
			for (Text key : sortedMap.keySet()) {
				if (counter++ == 10) {
					break;
				}
				context.write(key, new Text(sortedMap.get(key).toString()));
			}
		}
		private Map<Text, Double> sortByValues(Map<Text, Double> countMap1) {
			List<Text> keys = new ArrayList<>(countMap1.keySet());
			List<Double> values = new ArrayList<>(countMap1.values());
			Collections.sort(values);
			Collections.sort(keys);
			LinkedHashMap<Text, Double> sortedMap = new LinkedHashMap<>();
			Iterator<Double> valueIt = values.iterator();
			while (valueIt.hasNext()) {
				Double val = valueIt.next();
				Iterator<Text> keyIt = keys.iterator();
				while (keyIt.hasNext()) {
					Text key = keyIt.next();
					Double comp1 = countMap1.get(key);
					Double comp2 = val;
					if (comp1.equals(comp2)) {
						keyIt.remove();
						sortedMap.put(key, val);
						break;
					}
				}
			}
			return sortedMap;
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Q5");
		job.setJarByClass(Q5.class);
		job.setMapperClass(ReviewMap.class);
		job.setReducerClass(ReviewReduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(DoubleWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}



package WordCount.WordCount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountEmotions {

    public static class TokenizerMapper 
    extends Mapper<Object, Text, Text, IntWritable>{
    	private static Set<String> positivewords;
    	private static Set<String> negativewords;
    	private final static IntWritable one = new IntWritable(1);
    	private Text word = new Text();

        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException{
        	positivewords = new HashSet<String>();
        	negativewords = new HashSet<String>();
        	Configuration conf = context.getConfiguration();
			Path filePath=new Path(conf.get("positivefile"));
			FileSystem fs = FileSystem.get(URI.create(conf.get("positivefile")), conf);
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(filePath)));
			String line1;  
			while((line1 = br.readLine()) != null)
			{
				positivewords.add(line1);
				
			}
			filePath=new Path(conf.get("negativefile"));
			fs = FileSystem.get(URI.create(conf.get("negativefile")), conf);
			br=new BufferedReader(new InputStreamReader(fs.open(filePath)));
			String line2;  
			while((line2 = br.readLine()) != null)
			{
				negativewords.add(line2);
			}  	

        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                if(positivewords.contains(word.toString())){
                	context.write(new Text("Total count of positive words:"), one);
                }
                if(negativewords.contains(word.toString())){
                	context.write(new Text("Total count of negative words:"), one);
                }
            }
        }
    }

    public static class IntSumReducer 
    extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, 
                Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
		conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("positivefile", args[2]);
		conf.set("negativefile", args[3]);
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCountEmotions.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
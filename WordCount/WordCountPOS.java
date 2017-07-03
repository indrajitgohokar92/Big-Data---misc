package WordCount.WordCount;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountPOS {

	public static class TokenizerMapper 
	extends Mapper<Object, Text, IntWritable, MapWritable>{
		private final static IntWritable keyLength = new IntWritable(1);
		private Text word = new Text();
		private Map<String,String> hm;
		@SuppressWarnings("rawtypes")
		@Override
		protected void setup(Mapper.Context context) throws IOException, InterruptedException{
			hm = new HashMap<String, String>();
			Configuration conf = context.getConfiguration();
			Path filePath=new Path(conf.get("inpfile"));
			FileSystem fs = FileSystem.get(URI.create(conf.get("inpfile")), conf);
			BufferedReader br=new BufferedReader(new InputStreamReader(fs.open(filePath)));
			String line1;  
			while((line1 = br.readLine()) != null)
			{
				String[] sp=line1.split("\\\\");
				hm.put(sp[0], sp[1]);
			}
		}


		public void map(Object key, Text value, Context context
				) throws IOException, InterruptedException {
			StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				word.set(itr.nextToken());
				if(word.toString().length() > 4){
					if(hm.containsKey(word.toString())){
						String po=hm.get(word.toString());
						char p= po.charAt(0);
						if(p=='N'||p=='h'||p=='V'||p=='t'||p=='i'||p=='A'
								||p=='v'||p=='C'||p=='P'||p=='!'||p=='r'){
							IntWritable palindrome = new IntWritable();
							MapWritable res = new MapWritable();
							keyLength.set(word.toString().length()); 
							Text pos;
							if(p=='N'||p=='h')
								pos=new Text("Noun");
							else if(p=='P')
								pos=new Text("Preposition");
							else if(p=='V'||p=='t'||p=='i')
								pos=new Text("Verb");
							else if(p=='A')
								pos=new Text("Adjective");
							else if(p=='v')
								pos=new Text("Adverb");
							else if(p=='C')
								pos=new Text("Conjunction");
							else if(p=='!')
								pos=new Text("Interjection");
							else
								pos=new Text("Pronoun");

							if(word.toString().equals(new StringBuilder(word.toString()).reverse().toString()))
								palindrome.set(1);
							else
								palindrome.set(0);
							res.put(pos, palindrome);
							context.write(keyLength, res);
						}
					}
				}
			}
		}
	}

	public static class IntSumReducer 
	extends Reducer<IntWritable,MapWritable,Text,HashMap<Text,HashMap<Text,HashMap<Text, IntWritable>>>> {
		private HashMap<Text,HashMap<Text, IntWritable>> intermediate;
		private HashMap<Text,HashMap<Text,HashMap<Text, IntWritable>>> result;
		private Text count_txt = new Text();
		private Text palindromes_txt = new Text();
		public void reduce(IntWritable key, Iterable<MapWritable> values, 
				Context context
				) throws IOException, InterruptedException {
			intermediate = new HashMap<Text,HashMap<Text, IntWritable>>();
			result = new HashMap<Text,HashMap<Text,HashMap<Text, IntWritable>>>();
			HashMap<Text, IntWritable> po=new HashMap<Text, IntWritable>();
			int sum = 0;
			int palindrome=0;
			for (MapWritable entry : values) {
				IntWritable numberOfOccurs;
				for (Entry<Writable, Writable> extractData: entry.entrySet()) {
					Text pos=(Text) extractData.getKey();
					if(extractData.getValue().equals(new IntWritable(1))){
						palindrome+=1;
					}
					numberOfOccurs=(IntWritable) po.get(extractData.getKey());
					IntWritable newNumberOfOccurs;
					if(numberOfOccurs==null){
						newNumberOfOccurs = new IntWritable(1);
					}else{
						newNumberOfOccurs = new IntWritable(numberOfOccurs.get() + 1);
					}
					po.put(pos, newNumberOfOccurs);
				}    				
				sum += 1;
			}
			count_txt = new Text("Total Count: "+sum+" ");		
			palindromes_txt = new Text("Palindromes: "+palindrome+" ");
			intermediate.put(palindromes_txt, po);
			result.put(count_txt, intermediate);
			context.write(new Text("Length:"+key), result);
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("mapred.job.tracker", "hdfs://cshadoop1:61120");
		conf.set("yarn.resourcemanager.address", "cshadoop1.utdallas.edu:8032");
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("inpfile", args[2]);
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(WordCountEmotions.class);
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(MapWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(HashMap.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
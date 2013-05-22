import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashSet;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.map.InverseMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;



public class MapRed {
	
	private static HashSet<String> stopwords=new HashSet<String>();
	
	
	
	public static class 	TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		
		private static final IntWritable one=new IntWritable(1);
		private Text word=new Text();
		private final String Regex="[^\\w]";
		
		
		// read all the stop words
		@Override
		protected void setup(Context context) throws IOException,
				InterruptedException {
			// TODO Auto-generated method stub
			Path[] paths=context.getLocalCacheFiles();
			if(paths!=null && paths.length>0){
				BufferedReader fReader=new BufferedReader(new FileReader(new File(paths[0].toString())));
				String temp=fReader.readLine();
				while(temp!=null){
					stopwords.add(temp);
					temp=fReader.readLine();
				}
			}
		}



		public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
			String line=value.toString().replaceAll(Regex, " ");
			for (String pattern:stopwords)
				line=line.replaceAll(pattern, "");
			StringTokenizer st=new StringTokenizer(line);
			if(st.hasMoreElements()){
				String token=st.nextToken();
				word.set(token);
				context.write(word,one);
				
			}
			
		}
		
		
	}
	
	
	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		private IntWritable result=new IntWritable();
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
			int sum=0;
			for (IntWritable value:values){
				sum+=value.get();
			}
			result.set(sum);
			context.write(key, result); 
		}
		
	}
	
	
	public static class DecreaseComparator extends IntWritable.Comparator{

		@Override
		public int compare(WritableComparable a, WritableComparable b) {
			// TODO Auto-generated method stub
			return -super.compare(a, b);
		}
		
	}

	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException{
		
		String tempdir="wordcount_temp"+new Random().nextInt(1990);
		Configuration conf=new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();	 
	    if (otherArgs.length != 3) 
	    {    System.err.println("args error!");
	         System.exit(2);
	    } 
	    
		Job job=new Job(conf, "my newwordcount");
		job.addCacheFile(URI.create(otherArgs[2]));
		job.setJarByClass(MapRed.class);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setCombinerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(tempdir));
		if(job.waitForCompletion(true)){
			Job sortjob=new Job(conf, "sortjob");
			sortjob.setJarByClass(MapRed.class);
			sortjob.setMapperClass(InverseMapper.class);
			sortjob.setOutputKeyClass(IntWritable.class);
			sortjob.setOutputValueClass(Text.class);
			sortjob.setSortComparatorClass(DecreaseComparator.class);
			sortjob.setInputFormatClass(SequenceFileInputFormat.class);
			FileInputFormat.addInputPath(sortjob, new Path(tempdir));
			FileOutputFormat.setOutputPath(sortjob, new Path(otherArgs[1]));
			if(sortjob.waitForCompletion(true)){
				FileSystem.get(conf).delete(new Path(tempdir));
				System.exit(0);
			}
			System.exit(1);
		}
		System.exit(1);
		
	}
	
	
}
	
	


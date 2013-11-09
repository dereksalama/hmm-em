package edu;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class WordCount {
	
	private static Logger LOGGER = Logger.getLogger("InfoLogging");

	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, EMModelParameter> {
		private Text word = new Text();

		public void map(LongWritable key, Text value, OutputCollector<Text, EMModelParameter> output, Reporter reporter) throws IOException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				
				EMModelParameter transitionParameter = EMModelParameter.transitionParameter(word.toString(), "hi");
				output.collect(word, transitionParameter);
			}
		}
	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, EMModelParameter, Text, EMModelParameter> {
		public void reduce(Text key, Iterator<EMModelParameter> values, OutputCollector<Text, EMModelParameter> output, Reporter reporter) throws IOException {
			while (values.hasNext()) {
				output.collect(key, values.next());
			}
		}
	}

	public static void main(String[] args) throws Exception {		
		runIteration("WordCount iter", args[0], args[1], 5);
	}

	private static boolean runIteration(String jobName, String inputFilePath, String outputFilePath, int iteration) {
		JobConf conf = new JobConf(WordCount.class);
		conf.setJobName(jobName + "-" + iteration);

		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(EMModelParameter.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(EMModelParameter.class);
		
		FileInputFormat.setInputPaths(conf, new Path(inputFilePath));
		FileOutputFormat.setOutputPath(conf, new Path(outputFilePath + "-" + iteration));
	
		try {
			JobClient.runJob(conf);
//			FileSystem fs = FileSystem.get(outPath.toUri(), conf);
//			return isConverged(clustersOut, conf, fs);
		} catch (IOException e) {
			LOGGER.log(Level.WARNING, e.toString());
//			return true;
		}
		
		return true;
	}

}
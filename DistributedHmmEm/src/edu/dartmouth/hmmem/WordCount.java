package edu.dartmouth.hmmem;

import java.io.IOException;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class WordCount {
	
	private static Logger LOGGER = Logger.getLogger(WordCount.class.toString());

//	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, EMModelParameter> {
//		private Text word = new Text();
//
//		public void map(LongWritable key, Text value, OutputCollector<Text, EMModelParameter> output, Reporter reporter) throws IOException {
//			String line = value.toString();
//			StringTokenizer tokenizer = new StringTokenizer(line);
//			int i = 0;
//			while (tokenizer.hasMoreTokens()) {
//				i++;
//				
//				word.set(tokenizer.nextToken());
//				
//				EMModelParameter transitionParameter = new EMModelParameter(EMModelParameter.PARAMETER_TYPE_TRANSITION, word, word, i);
//				output.collect(word, transitionParameter);
//				
//				EMModelParameter emissionParameter = new EMModelParameter(EMModelParameter.PARAMETER_TYPE_EMISSION, word, word, -1 * i);
//				output.collect(word, emissionParameter);
//			}
//		}
//	}

	public static class Reduce extends MapReduceBase implements Reducer<Text, EMModelParameter, Text, Text> {
		public void reduce(Text key, Iterator<EMModelParameter> values, OutputCollector<Text, Text> output, Reporter reporter) throws IOException {
			
			LOGGER.log(Level.INFO, "Reduce");
			
			System.err.println("~~~~~~~~~~~~~Reduce~~~~~~~~~~~~~");
			
			while (values.hasNext()) {
				EMModelParameter modelParam = values.next();
				
				if (modelParam.getParameterType() == EMModelParameter.PARAMETER_TYPE_TRANSITION) {
					output.collect(key, new Text(modelParam.toString()));
				} else {
					output.collect(key, new Text(modelParam.toString()));
				}
			}
		}
	}
}
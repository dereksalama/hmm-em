package edu.dartmouth.hmmem;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * A MapReduce job for converting the parameters output by the EM iterations to a
 * human readable and exportable format.
 */
public class HumanReadableOutput {

	public static class HumanReadableOutputMapper extends MapReduceBase implements
			Mapper<LongWritable, EMModelParameter, Text, NullWritable> {

		@Override
		public void map(LongWritable key, EMModelParameter modelParam,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {
			output.collect(new Text(modelParam.toString()), NullWritable.get());
		}
	}
	
	public static class HumanReadableOutputReducer extends MapReduceBase implements
			Reducer<Text, NullWritable, Text, NullWritable> {

		@Override
		public void reduce(Text key, Iterator<NullWritable> values,
				OutputCollector<Text, NullWritable> output, Reporter reporter)
				throws IOException {
			output.collect(key, NullWritable.get());
		}
	}
}

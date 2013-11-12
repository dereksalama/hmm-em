package edu.dartmouth.hmmem;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ExpectationMapper extends MapReduceBase implements
		Mapper<LongWritable, EMModelParameter, Text, EMModelParameter> {
	
	public static final String PROBABILITIES_FILEPATH = "transitions";
	
	private final Map<StringPair, Double> transProbs = new HashMap<StringPair, Double>();
	private final Map<StringPair, Double> emitProbs = new HashMap<StringPair, Double>();
	

	@Override
	public void map(LongWritable key, EMModelParameter value,
			OutputCollector<Text, EMModelParameter> output, Reporter reporter)
			throws IOException {
		// TODO

	}
	
	@Override
	public void configure(JobConf job) {
		Path probFile = new Path(job.get(PROBABILITIES_FILEPATH));
		
		FSDataInputStream is = null;
		FileSystem fs = null;
		try {
			fs = FileSystem.get(new Configuration());
			is = fs.open(probFile);
			
			// TODO: figure out how to find end of file
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
		// TODO testing
		for (Entry<StringPair, Double> e : transProbs.entrySet()) {
			System.out.println(e.getKey() + ": " + e.getValue());
		}
		for (Entry<StringPair, Double> e : emitProbs.entrySet()) {
			System.out.println(e.getKey() + ": " + e.getValue());
		}
	}

}

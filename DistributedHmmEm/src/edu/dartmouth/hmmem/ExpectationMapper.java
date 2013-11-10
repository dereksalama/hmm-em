package edu.dartmouth.hmmem;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
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
	
	public static final String TRANSITIONS_FILEPATH = "transitions";
	public static final String EMISSIONS_FILEPATH = "emissions";
	
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
		Path transitionsFile = new Path(job.get(TRANSITIONS_FILEPATH));
		Path emissionsFile = new Path(job.get(EMISSIONS_FILEPATH));
		
		BufferedReader transBr = null;
		BufferedReader emitBr = null;
		FileSystem fs = null;
		try {
			fs = FileSystem.get(new Configuration());
			transBr = new BufferedReader(new InputStreamReader(fs.open(transitionsFile)));
			populateMap(transBr, transProbs);
			
			emitBr = new BufferedReader(new InputStreamReader(fs.open(emissionsFile)));
			populateMap(emitBr, emitProbs);
		} catch (IOException e) {
			if (transBr != null) {
				try {
					transBr.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
			if (emitBr != null) {
				try {
					emitBr.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
			if (fs != null) {
				try {
					fs.close();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
			}
			
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
	
	private void populateMap(BufferedReader br, Map<StringPair, Double> map) throws IOException {
		String line = br.readLine();
		while (line != null) {
			String[] vals = line.split("\\s+"); //split on whitespace
			if (vals.length != 3) {
				throw new IOException("Invalid file format");
			}
			Double logProb = Double.parseDouble(vals[2]);
			StringPair param = new StringPair(vals[0], vals[1]);
			map.put(param, logProb);
			
			line = br.readLine();
		}
	}

}

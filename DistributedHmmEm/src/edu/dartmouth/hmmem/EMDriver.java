package edu.dartmouth.hmmem;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

import edu.dartmouth.hmmem.WordCount.Reduce;

public class EMDriver {
	
	private static final Logger LOGGER = Logger.getLogger(EMDriver.class.toString());
	
	public static final String EM_MODEL_PARAMS_FILE_NAME = "em_model_params.txt";
	
	/**
	 * The main method that drives the distributed EM work flow.
	 * 
	 * Arguments:
	 * 0: Job name, e.g. "distributed-hmm-em"
	 * 1: The bucket URI, e.g. "s3n://distributed-hmm-em/" for the file system
	 * 2: Path to input directory containing emission sequences (one per line), e.g. "s3n://distributed-hmm-em/input"
	 * 3: Path to output directory that does not yet exist, e.g. "s3n://distributed-hmm-em/output-13"
	 * 4: Path to transitions file (each line of format "<from_state> <to_state>", with the first from_state
	 * 		being the symbol for the start of the emission sequence, e.g. "s3n://distributed-hmm-em/transitions.txt"
	 * 5: Path to emissions file (each line of format "<state> <token>", e.g. "s3n://distributed-hmm-em/emissions.txt"
	 * 6: Log convergence, i.e. difference between log alpha of EM iterations before final output is produced,
	 * 		e.g. "0.00001"
	 * 
	 * The main method first parses the input transition and emissions to generate
	 * a random seed for the model parameters. Then, the method spawns MapReduce steps
	 * that each perform one EM iteration until the difference between the log alphas
	 * after each iteration is less than the given convergence argument.
	 */
	public static void main(String args[]) throws Exception {
		System.err.println("~~~~~~~~~~~~~EMDriver~~~~~~~~~~~~~");
		
		// Obtain arguments in useful forms.
		if (args.length != 7) {
			for (int i = 0; i < args.length; i++) {
				System.err.println("\t" + i + ": " + args[i]);
			}
			throw new Exception("Exactly 7 arguments must be specified. The arguments given were:");
		}
				
		String jobName = args[0];
		
		String bucketURIStr = args[1];
		URI bucketURI = new URI(bucketURIStr);
		
		String inputDirPathStr = args[2];
		String outputDirPathStr = args[3];
		
		Path transFilePath = new Path(args[4]);
		Path emisFilePath = new Path(args[5]);
		
//		double logConvergence = Double.parseDouble(args[6]);
		
		// Create the random seed for the model parameters.
		FileSystem fs = NativeS3FileSystem.get(bucketURI, new Configuration());
		
		BufferedReader transFileReader = new BufferedReader(new InputStreamReader(fs.open(transFilePath)));
		Map<StringPair, Double> transLogProbMap = parsePairFile(transFileReader);
		transFileReader.close();
		
		BufferedReader emisFileReader = new BufferedReader(new InputStreamReader(fs.open(emisFilePath)));
		Map<StringPair, Double> emisLogProbMap = parsePairFile(emisFileReader);
		emisFileReader.close();
		
		// Output the random seed to a file to begin the EM.
		Path randomModelParamsSeedPath = new Path(outputDirPathStr + "/0/" + EM_MODEL_PARAMS_FILE_NAME);
		FSDataOutputStream randomModelParamsOut = fs.create(randomModelParamsSeedPath, false);
		outputEMModelParams(transLogProbMap, emisLogProbMap, randomModelParamsOut);
		randomModelParamsOut.close();
		
		///
		// Conduct the EM.
		LOGGER.log(Level.INFO, "Running an EM iteration!");
		runIteration(jobName, bucketURIStr, inputDirPathStr, outputDirPathStr, 1);
		
		// Write the final output.
		
		
		fs.close();
	}
	
	/**
	 * Parses the given pair file, where each line is of the form
	 * "<from_state> <to_state>" (for transition files) or "<state> <token>" for emission files.
	 * Assigns a random probability to each transition/emission
	 * and normalizes such that the sum of all transitions/emissions from a given state
	 * is 1.0. Returns a dictionary mapping the tuple to the log
	 * of the randomized and normalized probability.
	 */
	public static Map<StringPair, Double> parsePairFile(BufferedReader fileReader) throws Exception {
		Random random = new Random();
		Map<StringPair, Double> logProbMap = new HashMap<StringPair, Double>();
		
		String line;
		int i = 0;
		while (null != (line = fileReader.readLine())) {
			i++;
			
			// Skip blank lines.
			if (line.length() == 0) {
				continue;
			}
			
			String[] tokens = line.trim().split("\\s+");
			if (tokens.length != 2) {
				throw new Exception("Line " + i + " does not have exactly 2 tokens.");
			}
			
			String fromState = tokens[0];
			String toState = tokens[1];

			StringPair stringPair = new StringPair(fromState, toState);
			double logProb = random.nextDouble();

			if (logProbMap.containsKey(stringPair)) {
				throw new Exception("Line " + i + " is a duplicate of another line.");
			}
			
			logProbMap.put(stringPair, logProb);
		}
		
		normalizeLogProbMap(logProbMap);
		
		return logProbMap;
	}
	
	/**
	 * Normalizes a log probability map such that all the probabilities
	 * where the first string in the string pair key is some string x sum to 1.0.
	 */
	public static void normalizeLogProbMap(Map<StringPair, Double> logProbMap) {
		Map<String, Double> logProbSumMap = new HashMap<String, Double>();
		
		// See how much we have to scale down by to normalize.
		for (StringPair stringPair : logProbMap.keySet()) {
			String x = stringPair.x;
			
			Double logProb = logProbMap.get(stringPair);
			Double prevLogProbSum = logProbSumMap.get(x);
					
			Double newLogProbSum = calcLogSumOfLogs(logProb, prevLogProbSum);
			logProbSumMap.put(x, newLogProbSum);
		}
		
		// Do the normalization.
		for (StringPair stringPair : logProbMap.keySet()) {
			Double prevLogProb = logProbMap.get(stringPair);
			Double logProbSum = logProbSumMap.get(stringPair.x);
			
			if (prevLogProb != null) {
				Double normLogProb = prevLogProb - logProbSum;
				logProbMap.put(stringPair, normLogProb);
			}
		}
	}
	
	/**
	 * Given two numbers log(x) and log(y), returns log(x+y). If both log(x) and log(y) are negative,
	 * calculates the log of the sum in the following manner to reduce the risk of underflow:
	 * Finds log(z) = min(log(x), log(y)). Then adds log(z) to both log(x) and log(y) before
	 * exponentiating, adds the two exponentiations, takes the log of the sum, and then
	 * subtracts log(z). This calculation works properly because z*x + z*y = z*(x+y),
	 * so 2^(log(z)+log(x)) + 2^(log(z)+log(y)) = 2^(log(z)+log(x+y)).
	 * Thus, log(2^(log(z)+log(x)) + 2^(log(z)+log(y))) = log(2^(log(z)+log(x+y))) = log(z)+log(x+y).
	 *
	 * Note: If logx (logy) is passed as null, then x (y) is interpreted as 0. If both parameters
	 * are passed as null, then null is returned.
	 */
	public static Double calcLogSumOfLogs(Double logX, Double logY) {
		if (logX == null && logY == null) {
			return null;
		} else if (logX == null) {
			return logY;
		} else if (logY == null) {
			return logX;
		}
		
		if (logX >= 0 || logY >= 0) {
			double x = Math.pow(2, logX);
			double y = Math.pow(2, logY);
			
			double logSum = Math.log(x + y) / Math.log(2);
			return logSum;
		}
		
		double logZ = Math.min(logX, logY);
		double scaledLogX = logX + logZ;
		double scaledLogY = logY + logZ;
		
		double scaledX = Math.pow(2, scaledLogX);
		double scaledY = Math.pow(2, scaledLogY);
		
		double logScaledSum = Math.log(scaledX + scaledY) / Math.log(2);
		double logSum = logScaledSum - logZ;
		
		return logSum;
	}
	
	/**
	 * Outputs the given transition log prob map to the given file writer.
	 */
	public static void outputTransLogProbMap(Map<StringPair, Double> transLogProbMap, BufferedWriter fileWriter) {
		
	}
	
	/**
	 * Outputs the given transition and emission log prob maps in the form of serialized EMModelParameters to the given DataOutput.
	 */
	public static void outputEMModelParams(Map<StringPair, Double> transLogProbMap, Map<StringPair, Double> emisLogProbMap, DataOutput out) throws IOException {
		for (StringPair stringPair : transLogProbMap.keySet()) {
			EMModelParameter transModelParam = new EMModelParameter(EMModelParameter.PARAMETER_TYPE_TRANSITION,
					new Text(stringPair.x), new Text(stringPair.y), transLogProbMap.get(stringPair));
			transModelParam.write(out);
		}
		
		for (StringPair stringPair : emisLogProbMap.keySet()) {
			EMModelParameter emisModelParam = new EMModelParameter(EMModelParameter.PARAMETER_TYPE_EMISSION,
					new Text(stringPair.x), new Text(stringPair.y), emisLogProbMap.get(stringPair));
			emisModelParam.write(out);
		}
	}
	
	/**
	 * Conduct a single iteration of EM. Returns true if the algorithm has converged.
	 */
	private static boolean runIteration(String jobName, String bucketURIStr, String inputDirPathStr,
			String outputDirPathStr, int iteration) throws IOException {
		JobConf conf = new JobConf(EMDriver.class);
		conf.setJobName(jobName + "-" + iteration);

		conf.setMapperClass(ExpectationMapper.class);
		conf.setReducerClass(Reduce.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(EMModelParameter.class);
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(conf, new Path(inputDirPathStr));
		FileOutputFormat.setOutputPath(conf, new Path(outputDirPathStr + "/" + iteration + "/"));
		
		conf.set(ExpectationMapper.BUCKET_URI_KEY, bucketURIStr);
		
		String modelParamsDirPathStr = outputDirPathStr + "/" + (iteration-1) + "/";
		conf.set(ExpectationMapper.MODEL_PARAMETERS_DIR_PATH_KEY, modelParamsDirPathStr);
		
		JobClient.runJob(conf);
		
		return true;
	}
}
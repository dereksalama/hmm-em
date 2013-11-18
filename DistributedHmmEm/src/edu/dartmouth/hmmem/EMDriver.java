package edu.dartmouth.hmmem;

import java.io.BufferedReader;
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
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class EMDriver {
	
	private static final Logger LOGGER = Logger.getLogger(EMDriver.class.toString());
	
	public static final String EM_MODEL_PARAMS_FILE_NAME = "em_model_params.txt";
	
	private static String startState = null;
	
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
	 * 7: Max number of EM iterations, or -1 for no maximum.
	 * 
	 * The main method first parses the input transition and emissions to generate
	 * a random seed for the model parameters. Then, the method spawns MapReduce steps
	 * that each perform one EM iteration until the difference between the log alphas
	 * after each iteration is less than the given convergence argument.
	 */
	public static void main(String args[]) throws Exception {
		System.err.println("~~~~~~~~~~~~~EMDriver~~~~~~~~~~~~~");
		
		// Obtain arguments in useful forms.
		if (args.length != 8) {
			System.err.println("Exactly 8 arguments must be specified. The arguments given were:");
			for (int i = 0; i < args.length; i++) {
				System.err.println("\t" + i + ": " + args[i]);
			}
			throw new Exception("Exactly 8 arguments must be specified. " + args.length + " were passed.");
		}
				
		String jobName = args[0];
		
		String bucketURIStr = args[1];
		URI bucketURI = new URI(bucketURIStr);
		
		String inputDirPathStr = args[2];
		String outputDirPathStr = args[3];
		
		Path transFilePath = new Path(args[4]);
		Path emisFilePath = new Path(args[5]);
		
		double logConvergence = Double.parseDouble(args[6]);
		int maxIterations = Integer.parseInt(args[7]);
		
		// Create the random seed for the model parameters.
		FileSystem fs = NativeS3FileSystem.get(bucketURI, new Configuration());
		
		BufferedReader transFileReader = new BufferedReader(new InputStreamReader(fs.open(transFilePath)));
		Map<StringPair, Double> transLogProbMap = parsePairFile(transFileReader, true);
		transFileReader.close();
		
		BufferedReader emisFileReader = new BufferedReader(new InputStreamReader(fs.open(emisFilePath)));
		Map<StringPair, Double> emisLogProbMap = parsePairFile(emisFileReader, false);
		emisFileReader.close();
		
		// Output the random seed to a file to begin the EM.
		Path randomModelParamsSeedPath = new Path(outputDirPathStr + "/0/" + EM_MODEL_PARAMS_FILE_NAME);
		FSDataOutputStream randomModelParamsOut = fs.create(randomModelParamsSeedPath, false);
		outputEMModelParams(transLogProbMap, emisLogProbMap, randomModelParamsOut);
		randomModelParamsOut.close();
		
		// Conduct the EM.
		for (int i = 1; i <= maxIterations || maxIterations < 0; i++) { // Start iteration at 1 because initial parameters are at .../0/
			LOGGER.log(Level.INFO, "Running EM iteration " + i + "!");

			runIteration(jobName, bucketURIStr, inputDirPathStr, outputDirPathStr, startState, i);
		}
		
//		// Write the final output.
//		if (finalIteration >= 0) {
//			String emIterationOutputPathStr = outputDirPathStr + "/" + finalIteration + "/";
//			String humanReadableOutputPathStr = outputDirPathStr + "/human-readable/";
//			convertIterationOutputToHumanReadableFormat(fs, emIterationOutputPathStr, humanReadableOutputPathStr);
//		}
		
		fs.close();
	}
	
	/**
	 * Parses the given pair file, where each line is of the form
	 * "<from_state> <to_state>" (for transition files) or "<state> <token>" for emission files.
	 * Assigns a random probability to each transition/emission
	 * and normalizes such that the sum of all transitions/emissions from a given state
	 * is 1.0. Returns a dictionary mapping the tuple to the log
	 * of the randomized and normalized probability.
	 * 
	 * If isTransFile is set, indicating we are parsing a transition file, then the fromState of the
	 * first transition is used as the start state for all emission sequences.
	 */
	private static Map<StringPair, Double> parsePairFile(BufferedReader fileReader, boolean isTransFile) throws Exception {
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
			
			if (isTransFile && startState == null) {
				startState = stringPair.x;
			}
		}
		
		StaticUtil.normalizeLogProbMap(logProbMap);
		
		return logProbMap;
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
	 * Outputs the given transition and emission log prob maps in the form of serialized EMModelParameters to the given DataOutput.
	 */
	public static void outputEMModelParams(Map<StringPair, Double> transLogProbMap, Map<StringPair, Double> emisLogProbMap, DataOutput out) throws IOException {
		for (StringPair stringPair : transLogProbMap.keySet()) {
			EMModelParameter transModelParam = new EMModelParameter(EMModelParameter.PARAMETER_TYPE_TRANSITION,
					new Text(stringPair.x), new Text(stringPair.y), transLogProbMap.get(stringPair));
			out.writeChars(transModelParam.toString() + "\n");
		}
		
		for (StringPair stringPair : emisLogProbMap.keySet()) {
			EMModelParameter emisModelParam = new EMModelParameter(EMModelParameter.PARAMETER_TYPE_EMISSION,
					new Text(stringPair.x), new Text(stringPair.y), emisLogProbMap.get(stringPair));
			out.writeChars(emisModelParam.toString() + "\n");
		}
	}
	
	/**
	 * Conduct a single iteration of EM. Returns true if the algorithm has converged.
	 */
	private static boolean runIteration(String jobName, String bucketURIStr, String inputDirPathStr,
			String outputDirPathStr, String startState, int iteration) throws IOException {
		JobConf conf = new JobConf(EMDriver.class);
		conf.setJobName(jobName + "-" + iteration);

		conf.setMapperClass(ExpectationMapper.class);
		conf.setReducerClass(MaximizationReducer.class);

		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(EMModelParameter.class);
		conf.setOutputKeyClass(NullWritable.class);
		conf.setOutputValueClass(EMModelParameter.class);
		
		FileInputFormat.setInputPaths(conf, new Path(inputDirPathStr));
		FileOutputFormat.setOutputPath(conf, new Path(outputDirPathStr + "/" + iteration + "/"));
		
		conf.set(ExpectationMapper.BUCKET_URI_KEY, bucketURIStr);
		
		String modelParamsDirPathStr = outputDirPathStr + "/" + (iteration-1) + "/";
		conf.set(ExpectationMapper.MODEL_PARAMETERS_DIR_PATH_KEY, modelParamsDirPathStr);
		
		conf.set(ExpectationMapper.START_STATE_KEY, startState);
		
		JobClient.runJob(conf);
		
		// TODO: Check for convergence.
		return true;
	}
	
//	/**
//	 * Convert the output of an EM iteration to a human readable and exportable format.
//	 */
//	private static void convertIterationOutputToHumanReadableFormat(FileSystem fs, String emIterationOutputPathStr, String humanReadableOutputPathStr) throws Exception {
//		Map<StringPair, Double> transLogProbMap = new HashMap<StringPair, Double>();
//		Map<StringPair, Double> emisLogProbMap = new HashMap<StringPair, Double>();
//		
//		FileStatus[] modelParameterFileStatuses;
//		modelParameterFileStatuses = fs.listStatus(new Path(emIterationOutputPathStr));
//
//		for (FileStatus modelParameterFileStatus : modelParameterFileStatuses) {
//			LOGGER.log(Level.INFO, "Parsing model parameters file: " + modelParameterFileStatus.getPath());
//
//			if (!modelParameterFileStatus.getPath().getName().equals(MaximizationReducer.TOTAL_LOG_ALPHA_FILE_NAME)) {
//				DataInput modelParametersIn = fs.open(modelParameterFileStatus.getPath());
//				StaticUtil.readModelParametersFile(modelParametersIn, transLogProbMap, emisLogProbMap);
//			}
//		}
//		
//		// Output the model parameters in human readable form.
//		Path humanReadableTransOutputPath = new Path(humanReadableOutputPathStr + "/trans.txt");
//		Path humanReadableEmisOutputPath = new Path(humanReadableOutputPathStr + "/emis.txt");
//		
//		FSDataOutputStream humanReadableTransOut = fs.create(humanReadableTransOutputPath, false);
//		FSDataOutputStream humanReadableEmisOut = fs.create(humanReadableEmisOutputPath, false);
//		
//		outputHumanReadablePairProbDict(transLogProbMap, humanReadableTransOut);
//		outputHumanReadablePairProbDict(emisLogProbMap, humanReadableEmisOut);
//		
//		humanReadableTransOut.close();
//		humanReadableEmisOut.close();
//	}
//	
//	/**
//	 * Outputs trans and emis files in human readable format:
//	 * 
//	 * Trans -
//	 * <from_state> <to_state> <log_prob>
//	 * 
//	 * Emis -
//	 * <state> <token> <log_prob>
//	 */
//	private static void outputHumanReadablePairProbDict(Map<StringPair, Double> pairProbDict, FSDataOutputStream out) throws IOException {
//		for (Entry<StringPair, Double> entry : pairProbDict.entrySet()) {
//			String line = entry.getKey().x + " " + entry.getKey().y + " " + entry.getValue() + "\n";
//			out.writeChars(line);
//		}
//	}
}

package edu.dartmouth.hmmem;

import java.io.BufferedReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class EMDriver {

	/**
	 * Parses the given transition file, where each line is of the form
	 * "<from_state> <to_state>". Assigns a random probability to each transition
	 * and normalizes such that the sum of all transitions from a given from_state
	 * is 1.0. Returns a dictionary mapping the from_state to_state tuple to the log
	 * of the randomized and normalized probability.
	 */
	public static Map<StringPair, Double> parseTransitionFile(BufferedReader transFileReader) throws Exception {
		Random random = new Random();
		Map<StringPair, Double> transLogProbMap = new HashMap<StringPair, Double>();
		
		String line;
		int i = 0;
		while (null != (line = transFileReader.readLine())) {
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

			if (transLogProbMap.containsKey(stringPair)) {
				throw new Exception("Line " + i + " is a duplicate of another line.");
			}
			
			transLogProbMap.put(stringPair, logProb);
		}
		
		normalizeLogProbMap(transLogProbMap);
		
		return transLogProbMap;
	}
	
	/**
	 * Parses the given emission file, where each line is of the form
	 * "<state> <token>". Assigns a random probability to each emission
	 * and normalizes such that the sum of all emissions from a given state
	 * is 1.0. Returns a dictionary mapping the (state, token) tuple to the log
	 * of the randomized and normalized probability.
	 */
	public static Map<StringPair, Double> parseEmissionFile(BufferedReader emisFileReader) throws Exception {
		Random random = new Random();
		Map<StringPair, Double> emisLogProbMap = new HashMap<StringPair, Double>();
		
		String line;
		int i = 0;
		while (null != (line = emisFileReader.readLine())) {
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

			if (emisLogProbMap.containsKey(stringPair)) {
				throw new Exception("Line " + i + " is a duplicate of another line.");
			}
			
			emisLogProbMap.put(stringPair, logProb);
		}
		
		normalizeLogProbMap(emisLogProbMap);
		
		return emisLogProbMap;
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
}
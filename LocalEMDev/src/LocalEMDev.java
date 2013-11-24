import java.io.BufferedReader;
import java.io.FileReader;
import java.io.PrintWriter;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;

import edu.dartmouth.hmmem.StaticUtil;
import edu.dartmouth.hmmem.StringPair;
import edu.dartmouth.hmmem.TaggedObservationSequence;

public class LocalEMDev {

	public static final int MAX_ITERATIONS = 10;

	public static void main(String args[]) throws Exception {
		String emisFile = args[0];
		String transFile = args[1];
		String corpusFile = args[2];
		String outputFile = args[3];
		
		///////////////////////////
		// Testing normalization //
		
		Map<StringPair, Double> testLogProbMap = new HashMap<StringPair, Double>();
		testLogProbMap.put(new StringPair("A", "x"), 1.2693930907800875E-79);
		testLogProbMap.put(new StringPair("A", "y"), 1.2693930907800875E-79);
		
		normalizeLogProbMap(testLogProbMap);
		
		printParameterMap(testLogProbMap);
		
		///////////////////////////
		
		BufferedReader transFileReader = new BufferedReader(new FileReader(emisFile));
		Map<StringPair, Double> transLogProbDict = parsePairFile(transFileReader);

		//		transLogProbDict.put(new StringPair("#", "P"), Math.log(0.5) / Math.log(2));
		//		transLogProbDict.put(new StringPair("#", "U"), Math.log(0.5) / Math.log(2));
		//		transLogProbDict.put(new StringPair("P", "P"), Math.log(0.6) / Math.log(2));
		//		transLogProbDict.put(new StringPair("P", "U"), Math.log(0.4) / Math.log(2));
		//		transLogProbDict.put(new StringPair("U", "P"), Math.log(0.1) / Math.log(2));
		//		transLogProbDict.put(new StringPair("U", "U"), Math.log(0.9) / Math.log(2));
		//		
		//		transLogProbDict.put(new StringPair("#", "C"), Math.log(0.7) / Math.log(2));
		//		transLogProbDict.put(new StringPair("#", "V"), Math.log(0.3) / Math.log(2));
		//		transLogProbDict.put(new StringPair("C", "C"), Math.log(0.4) / Math.log(2));
		//		transLogProbDict.put(new StringPair("C", "V"), Math.log(0.6) / Math.log(2));
		//		transLogProbDict.put(new StringPair("V", "V"), Math.log(0.1) / Math.log(2));
		//		transLogProbDict.put(new StringPair("V", "C"), Math.log(0.9) / Math.log(2));

		BufferedReader emisFileReader = new BufferedReader(new FileReader(transFile));
		Map<StringPair, Double> emisLogProbDict = parsePairFile(emisFileReader);

		//		emisLogProbDict.put(new StringPair("P", "S"), Math.log(0.2) / Math.log(2));
		//		emisLogProbDict.put(new StringPair("U", "S"), Math.log(0.7) / Math.log(2));
		//		emisLogProbDict.put(new StringPair("P", "T"), Math.log(0.5) / Math.log(2));
		//		emisLogProbDict.put(new StringPair("U", "T"), Math.log(0.1) / Math.log(2));
		//		emisLogProbDict.put(new StringPair("P", "M"), Math.log(0.3) / Math.log(2));
		//		emisLogProbDict.put(new StringPair("U", "M"), Math.log(0.2) / Math.log(2));
		//		
		//		emisLogProbDict.put(new StringPair("C", "b"), Math.log(0.09) / Math.log(2));
		//		emisLogProbDict.put(new StringPair("V", "b"), Math.log(0.01) / Math.log(2));
		//		emisLogProbDict.put(new StringPair("C", "a"), Math.log(0.02) / Math.log(2));
		//		emisLogProbDict.put(new StringPair("V", "a"), Math.log(0.14) / Math.log(2));
		//		emisLogProbDict.put(new StringPair("C", "r"), Math.log(0.07) / Math.log(2));
		//		emisLogProbDict.put(new StringPair("V", "r"), Math.log(0.03) / Math.log(2));

		//		StaticUtil.readModelParametersFile(new RandomAccessFile("/Users/jakeleichtling/Desktop/test/params.txt", "r"), transLogProbDict, emisLogProbDict);

		Set<String> stateSet = new HashSet<String>();
		for (StringPair transStringPair : transLogProbDict.keySet()) {
			stateSet.add(transStringPair.getY());
		}

		// System.out.println(stateSet);

		String startState = "#";

		BufferedReader corpusReader = new BufferedReader(new FileReader(corpusFile));
		List<String> lines = new LinkedList<String>();
		for (int lineNumber = 0; lineNumber < 3; lineNumber++) {
			lines.add(corpusReader.readLine());
		}
		corpusReader.close();
		
		List<List<String>> obsSequences = new LinkedList<List<String>>();
		for (String line : lines) {
			if (line.isEmpty()) {
				continue;
			}
			List<String> obsSequence = Arrays.asList(line.trim().split("\\s+"));
			obsSequences.add(obsSequence);
		}

		for (int i = 0; i < MAX_ITERATIONS; i++) {
			Map<StringPair, Double> nextTransLogDict = new HashMap<StringPair, Double>();
			Map<StringPair, Double> nextEmisLogDict = new HashMap<StringPair, Double>();	
			Double totalLogAlpha = 0.0;
			
			for (List<String> obsSequence : obsSequences) {

				// System.out.println("Viterbi tagging:");
				// System.out.println(calculateViterbiTagging(firstLineObsSequence, transLogProbDict, emisLogProbDict, stateSet, startState));		

				Map<String, Double[]> forwardMatrix = calculateForwardMatrix(obsSequence, transLogProbDict, emisLogProbDict, stateSet, startState);
				// System.out.println("Forward matrix:");
//				printForwardOrBackwardMatrix(forwardMatrix, obsSequence);

				Map<String, Double[]> backwardMatrix = calculateBackwardMatrix(obsSequence, transLogProbDict, emisLogProbDict, stateSet, startState);
				// System.out.println("\nBackward matrix:");
//				printForwardOrBackwardMatrix(backwardMatrix, obsSequence);

//				System.out.println("\nLog alpha:");
//				System.out.println(getLogAlpha(firstLineForwardMatrix));
				Double logAlpha = getLogAlpha(forwardMatrix);
				System.out.println("Line:");
				System.out.println(obsSequence.toString());
				System.out.println("Log alpha:");
				System.out.println(logAlpha);
				if (logAlpha == null) {
					System.err.println("NULL LOG ALPHA!");
				}
				totalLogAlpha = calcLogProductOfLogs(totalLogAlpha, logAlpha);
				
				Map<StringPair, Double> logTransCounts = calculateLogTransitionCounts(forwardMatrix, backwardMatrix, logAlpha, transLogProbDict, emisLogProbDict, obsSequence, stateSet, startState);
//				normalizeLogProbMap(logTransCounts);
				// System.out.println("\nTransition counts:");
//				printParameterMap(logTransCounts);
				addLogPairDicts(nextTransLogDict, logTransCounts);

				Map<StringPair, Double> logEmisCounts = calculateLogEmissionCounts(forwardMatrix, backwardMatrix, logAlpha, obsSequence, stateSet);
//				normalizeLogProbMap(logEmisCounts);
				// System.out.println("\nEmission counts:");
//				printParameterMap(logEmisCounts);
				addLogPairDicts(nextEmisLogDict, logEmisCounts);
			}
			
			normalizeLogProbMap(nextTransLogDict);
			normalizeLogProbMap(nextEmisLogDict);
			
			transLogProbDict = nextTransLogDict;
			emisLogProbDict = nextEmisLogDict;
			
			System.out.println("Total log alpha at iteration " + i + ": " + totalLogAlpha);
			

		}
		
//		System.out.println("Trans log prob dict: ");
//		printParameterMap(transLogProbDict);
//		
//		System.out.println("Emis log prob dict: ");
//		printParameterMap(emisLogProbDict);

		// Test that trans probs add to 1
		System.out.println("TEST");
		Map<String, Double> logProbSumMap = new HashMap<String, Double>();
		for (Entry<StringPair, Double> entry : transLogProbDict.entrySet()) {
			logProbSumMap.put(entry.getKey().getX(), calcLogSumOfLogs(entry.getValue(), logProbSumMap.get(entry.getKey().getX())));
		}
		for (Entry<String, Double> entry : logProbSumMap.entrySet()) {
			System.out.println(entry);
		}
		
		PrintWriter writer = new PrintWriter(outputFile, "UTF-8");
		
		for (List<String> obsSequence : obsSequences) {
			TaggedObservationSequence tagging = calculateViterbiTagging(obsSequence, transLogProbDict, emisLogProbDict, 
				stateSet, "#");
			writer.println(tagging.toString());
		}
		
		writer.close();
	}

	/**
	 * Calculates the forward matrix given an observation sequence and model parameters.
	 * Returns a dictionary mapping each state X to an array of log probabilities, where each
	 * log probability at index i corresponds to the sum over all possible previous taggings
	 * of the probability of observation i given this state X.
	 */
	public static Map<String, Double[]> calculateForwardMatrix(List<String> observationSequence,
			Map<StringPair, Double> transLogProbDict, Map<StringPair, Double> emisLogProbDict, Set<String> stateSet, String startState) {
		int numObs = observationSequence.size();
		if (numObs == 0) {
			return null;
		}

		Map<String, Double[]> forwardMatrix = new HashMap<String, Double[]>();

		for (String state : stateSet) {
			forwardMatrix.put(state, new Double[numObs]);
		}

		// Begin by filling out the matrix for the first observation. This requires
		// a special case because we start out at startState with a probability of 1.0.
		for (String state : stateSet) {
			// P(state|#) * P(firstObservation|state)
			StringPair transStringPair = new StringPair(startState, state);
			Double logProbStateGivenStart = transLogProbDict.get(transStringPair);

			StringPair emisStringPair = new StringPair(state, observationSequence.get(0));
			Double logProbObsGivenState = emisLogProbDict.get(emisStringPair);

			Double logProb = calcLogProductOfLogs(logProbStateGivenStart, logProbObsGivenState);
			forwardMatrix.get(state)[0] = logProb;
		}

		// Now complete the rest of the matrix.
		for (int i = 1; i < numObs; i++) {
			String obs = observationSequence.get(i);

			for (String state : stateSet) {
				Double totalLogProb = null; // Probability starts out as 0 and accumulates from all previous states.

				// The emission probability is the same given one state.
				StringPair emisStringPair = new StringPair(state, obs);
				Double logProbObsGivenState = emisLogProbDict.get(emisStringPair);

				for (String prevState : stateSet) {
					// P(state|prevState) * P(obs|state) * Forward(i-1, prevState)
					StringPair transStringPair = new StringPair(prevState, state);
					Double logProbStateGivenPrevState = transLogProbDict.get(transStringPair);

					Double prevObsPrevStateForwardLogProb = forwardMatrix.get(prevState)[i-1];

					Double newLogProb = calcLogProductOfLogs(calcLogProductOfLogs(logProbStateGivenPrevState, logProbObsGivenState), prevObsPrevStateForwardLogProb);
					totalLogProb = calcLogSumOfLogs(totalLogProb, newLogProb);
				}

				forwardMatrix.get(state)[i] = totalLogProb;
			}
		}

		return forwardMatrix;
	}

	/**
	 * Calculates the backward matrix given an observation sequence and model parameters.
	 * Returns a dictionary mapping each state X to an array of log probabilities, where each
	 * log probability at index i corresponds to the sum over all possible subsequent taggings
	 * given this state X, not including the observation i.
	 */
	public static Map<String, Double[]> calculateBackwardMatrix(List<String> observationSequence,
			Map<StringPair, Double> transLogProbDict, Map<StringPair, Double> emisLogProbDict, Set<String> stateSet, String startState) {
		int numObs = observationSequence.size();
		if (numObs == 0) {
			return null;
		}

		Map<String, Double[]> backwardMatrix = new HashMap<String, Double[]>();

		for (String state : stateSet) {
			backwardMatrix.put(state, new Double[numObs]);
		}

		// Begin by filling out the matrix for the last observation. This requires
		// a special case because we start out with a subsequent taggings probability of 1.0.
		for (String state : stateSet) {
			backwardMatrix.get(state)[numObs-1] = Math.log(1.0) / Math.log(2); 
		}

		// Now complete the rest of the matrix.
		for (int i = numObs - 2; i >= 0; i--) {
			String nextObs = observationSequence.get(i+1);

			for (String state : stateSet) {
				Double totalLogProb = null; // Probability starts out as 0 and accumulates from all next states.

				for (String nextState : stateSet) {
					// P(nextState|state) * P(nextObs|nextState) * Backward(i+1, nextState)
					StringPair transStringPair = new StringPair(state, nextState);
					Double logProbStateGivenPrevState = transLogProbDict.get(transStringPair);

					StringPair emisStringPair = new StringPair(nextState, nextObs);
					Double logProbNextObsGivenNextState = emisLogProbDict.get(emisStringPair);

					Double nextObsNextStateBackwardLogProb = backwardMatrix.get(nextState)[i+1];

					Double newLogProb = calcLogProductOfLogs(calcLogProductOfLogs(logProbStateGivenPrevState, logProbNextObsGivenNextState), nextObsNextStateBackwardLogProb);
					totalLogProb = calcLogSumOfLogs(totalLogProb, newLogProb);
				}

				backwardMatrix.get(state)[i] = totalLogProb;
			}
		}

		return backwardMatrix;
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
			Double logProb = random.nextDouble();

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
			String x = stringPair.getX();

			Double logProb = logProbMap.get(stringPair);
			Double prevLogProbSum = logProbSumMap.get(x);

			Double newLogProbSum = calcLogSumOfLogs(logProb, prevLogProbSum);
			logProbSumMap.put(x, newLogProbSum);
		}

		// Do the normalization.
		for (StringPair stringPair : logProbMap.keySet()) {
			Double prevLogProb = logProbMap.get(stringPair);
			Double logProbSum = logProbSumMap.get(stringPair.getX());

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

		double logZ = Math.max(logX, logY) * -1.0;
		double scaledLogX = logX + logZ;
		double scaledLogY = logY + logZ;

		double scaledX = Math.pow(2, scaledLogX);
		double scaledY = Math.pow(2, scaledLogY);

		double logScaledSum = Math.log(scaledX + scaledY) / Math.log(2);
		double logSum = logScaledSum - logZ;

		return logSum;
	}

	/**
	 * Given two numbers log(x) and log(y), returns log(x*y) = log(x) + log(y). This method is used
	 * because it handles the case where x or y is 0, which is represented by log(x) or log(y) being null.
	 * In this case, x*y == 0, so null is returned to represent log(0).
	 */
	public static Double calcLogProductOfLogs(Double logX, Double logY) {
		if (logX == null || logY == null) {
			return null;
		}

		return logX + logY;
	}

	// Test
	public static void printForwardOrBackwardMatrix(Map<String, Double[]> matrix, List<String> observationSequence) {
		DecimalFormat df = new DecimalFormat("#.#######");

		for (String obs : observationSequence) {
			// System.out.print("\t\t" + obs);
		}

		// System.out.println();

		for (Entry<String, Double[]> entry : matrix.entrySet()) {
			// System.out.print(entry.getKey());

			for (Double logProb : entry.getValue()) {
				// System.out.print("\t\t" + (logProb == null ? 0 : df.format(Math.pow(2, logProb))));
			}

			// System.out.println();
		}
	}

	/**
	 * Calculates the transition counts for an observation sequence.
	 */
	public static Map<StringPair, Double> calculateLogTransitionCounts(Map<String, Double[]> forwardMatrix, Map<String, Double[]> backwardMatrix, Double logAlpha,
			Map<StringPair, Double> transLogProbDict, Map<StringPair, Double> emisLogProbDict, List<String> observationSequence,
			Set<String> stateSet, String startState) {
		Map<StringPair, Double> logTransCounts = new HashMap<StringPair, Double>();

		int numObs = observationSequence.size();

		// Calculate the transition counts from #.
		for (String state : stateSet) {
			Double forwardLogProb = forwardMatrix.get(state)[0];
			Double backwardLogProb = backwardMatrix.get(state)[0];

			Double logProbStateGivenStart = calcLogProductOfLogs(forwardLogProb, backwardLogProb);
			StringPair transStringPair = new StringPair(startState, state);
			
			if (logProbStateGivenStart != null) {
				logTransCounts.put(transStringPair, logProbStateGivenStart);
			}
		}

		// Calculate transition counts for the rest of the transitions.
		for (int i = 0; i < numObs - 1; i++) {
			String nextObs = observationSequence.get(i+1);

			for (String fromState : stateSet) {
				Double forwardLogProb = forwardMatrix.get(fromState)[i];

				for (String toState : stateSet) {
					Double backwardLogProb = backwardMatrix.get(toState)[i+1];

					StringPair transStringPair = new StringPair(fromState, toState);
					Double transLogProb = transLogProbDict.get(transStringPair);

					StringPair emisStringPair = new StringPair(toState, nextObs);
					Double emisLogProb = emisLogProbDict.get(emisStringPair);

					Double logProbToStateGivenFromState = calcLogProductOfLogs(calcLogProductOfLogs(calcLogProductOfLogs(forwardLogProb, transLogProb), emisLogProb), backwardLogProb);					
					Double prevLogTransCount = logTransCounts.get(transStringPair);
					
					Double sumLogProbToStateGivenFromState = calcLogSumOfLogs(prevLogTransCount, logProbToStateGivenFromState);
					if (sumLogProbToStateGivenFromState != null) {
						logTransCounts.put(transStringPair, sumLogProbToStateGivenFromState);
					}
//					else {
//						logTransCounts.remove(transStringPair);
//					}

					// System.out.println("------------------------");
					// System.out.println("i = " + i);
					// System.out.println(transStringPair);
					// System.out.println("Forward: " + Math.pow(2, forwardLogProb));
					// System.out.println("Backward: " + Math.pow(2, backwardLogProb));
					// System.out.println("New: " + Math.pow(2, logProbToStateGivenFromState));
					// System.out.println("Total: " + Math.pow(2, logTransCounts.get(transStringPair)));
				}
			}
		}
		
		// Go through and divide all counts by alpha.
		for (Entry<StringPair, Double> entry : logTransCounts.entrySet()) {
			logTransCounts.put(entry.getKey(), entry.getValue() - logAlpha);
		}

		return logTransCounts;
	}

	/**
	 * Calculates the emission counts for an observation sequence.
	 */
	public static Map<StringPair, Double> calculateLogEmissionCounts(Map<String, Double[]> forwardMatrix, Map<String, Double[]> backwardMatrix, Double logAlpha,
			List<String> observationSequence, Set<String> stateSet) {
		Map<StringPair, Double> logEmisCounts = new HashMap<StringPair, Double>();

		int numObs = observationSequence.size();
		for (int i = 0; i < numObs; i++) {
			String obs = observationSequence.get(i);

			for (String state : stateSet) {
				Double forwardLogProb = forwardMatrix.get(state)[i];
				Double backwardLogProb = backwardMatrix.get(state)[i];

				Double logProbObsGivenState = calcLogProductOfLogs(forwardLogProb, backwardLogProb);
				StringPair emisStringPair = new StringPair(state, obs);
				Double prevLogEmisCount = logEmisCounts.get(emisStringPair);
				
				Double sumLogEmisCount = calcLogSumOfLogs(prevLogEmisCount, logProbObsGivenState);
				if (sumLogEmisCount != null) {
					logEmisCounts.put(emisStringPair, sumLogEmisCount);
				} 
//				else {
//					logEmisCounts.remove(emisStringPair);
//				}
			}
		}
		
		// Go through and divide all counts by alpha.
		for (Entry<StringPair, Double> entry : logEmisCounts.entrySet()) {
			logEmisCounts.put(entry.getKey(), entry.getValue() - logAlpha);
		}

		return logEmisCounts;
	}

	public static void printParameterMap(Map<StringPair, Double> paramMap) {
		for (Entry<StringPair, Double> entry : paramMap.entrySet()) {
			Double value = entry.getValue();
//			 System.out.println(entry.getKey() + " : " + (value == null ? "null" : Math.pow(2, value)));
			if (value != null) {
				Double prob = Math.pow(2, value);
				if (prob > .0001) {
					System.out.println(entry.getKey() + " : " + prob);
				}
			}
		}
	}

	/**
	 * Returns the log alpha for the observation sequence under the given model, or null if alpha == 0.
	 */
	private static Double getLogAlpha(Map<String, Double[]> forwardMatrix) {
		Double logAlpha = null;

		for (Entry<String, Double[]> entry : forwardMatrix.entrySet()) {
			Double[] logProbArray = entry.getValue();
			Double lastLogProb = entry.getValue()[logProbArray.length - 1];

			logAlpha = calcLogSumOfLogs(logAlpha, lastLogProb);
		}

		return logAlpha;
	}

	private static TaggedObservationSequence calculateViterbiTagging(List<String> observationSequence, Map<StringPair, Double> transLogProbDict,
			Map<StringPair, Double> emisLogProbDict, Set<String> stateSet, String startState) {
		int numObs = observationSequence.size();
		if (numObs == 0) {
			return null;
			// TODO: What happens next?
		}

		// Keeps track of probabilities.
		Map<String, Double[]> viterbiLogProbMatrix = new HashMap<String, Double[]>();
		// Keeps track of previous tag that maximized probability.
		Map<String, String[]> viterbiPrevStateMatrix = new HashMap<String, String[]>();

		for (String state : stateSet) {
			viterbiLogProbMatrix.put(state, new Double[numObs]);
			viterbiPrevStateMatrix.put(state, new String[numObs]);
		}

		// Begin by filling out the matrix for the first observation. This requires a
		// special case because we start out at startState with a probability of 1.0.
		for (String state : stateSet) {
			// P(state|#) * P(firstObservation|state)
			StringPair transStringPair = new StringPair(startState, state);
			Double logProbStateGivenStart = transLogProbDict.get(transStringPair);

			StringPair emisStringPair = new StringPair(state, observationSequence.get(0));
			Double logProbObsGivenState = emisLogProbDict.get(emisStringPair);

			Double logProb = StaticUtil.calcLogProductOfLogs(logProbStateGivenStart, logProbObsGivenState);
			viterbiLogProbMatrix.get(state)[0] = logProb;
		}

		// Now complete the rest of the matrix.
		for (int i = 1; i < numObs; i++) {
			String obs = observationSequence.get(i);

			for (String state : stateSet) {
				// The emission probability is the same given one state.
				StringPair emisStringPair = new StringPair(state, obs);
				Double logProbObsGivenState = emisLogProbDict.get(emisStringPair);

				for (String prevState : stateSet) {
					// P(state|prevState) * P(obs|state) * Forward(i-1, prevState)
					StringPair transStringPair = new StringPair(prevState, state);
					Double logProbStateGivenPrevState = transLogProbDict.get(transStringPair);

					Double prevObsPrevStateViterbiLogProb = viterbiLogProbMatrix.get(prevState)[i-1];

					Double logProb = StaticUtil.calcLogProductOfLogs(StaticUtil.calcLogProductOfLogs(logProbStateGivenPrevState, logProbObsGivenState), prevObsPrevStateViterbiLogProb);
					Double maxLogProb = viterbiLogProbMatrix.get(state)[i];
					if (maxLogProb == null || (logProb != null && logProb > maxLogProb)) {
						viterbiLogProbMatrix.get(state)[i] = logProb;
						viterbiPrevStateMatrix.get(state)[i] = prevState;
					}
				}
			}
		}

		// Find the tagging for the last emission in the sequence.
		Double lastMaxLogProb = null;
		String lastMaxState = null;
		for (String state : stateSet) {
			Double lastLogProb = viterbiLogProbMatrix.get(state)[numObs - 1];
			if (lastMaxLogProb == null || (lastLogProb != null && lastLogProb > lastMaxLogProb)) {
				lastMaxLogProb = lastLogProb;
				lastMaxState = state;
			}
		}

		// Create the optimal tagging from the matrices.
		TaggedObservationSequence optimalTagging = new TaggedObservationSequence();

		String state = lastMaxState;
		for (int i = numObs - 1; i >= 0; i--) {
			String obs = observationSequence.get(i);
			optimalTagging.prependObsTag(new StringPair(obs, state));

			// Update state for the preceding observation.
			state = viterbiPrevStateMatrix.get(state)[i];
		}

		return optimalTagging;
	}
	
	private static void addLogPairDicts(Map<StringPair, Double> accumulatorDict, Map<StringPair, Double> dictToAdd) {
		for (Entry<StringPair, Double> entry : dictToAdd.entrySet()) {
			if (accumulatorDict.containsKey(entry.getKey())) {
				accumulatorDict.put(entry.getKey(), calcLogSumOfLogs(entry.getValue(), accumulatorDict.get(entry.getKey())));
			} else {
				accumulatorDict.put(entry.getKey(), entry.getValue());
			}
		}
	}
}

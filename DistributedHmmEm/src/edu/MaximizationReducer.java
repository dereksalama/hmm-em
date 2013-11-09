package edu;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * Reducer to perform maximization step.
 * Input - 
 * key: state (either start of transition, or state for emission)
 * value: end state of transition or emission, expected count of event
 * 
 * Output -
 * key: either (start state, end state) or (state, emission)
 * value: probability
 * 
 * The reducer works by taking the expected counts for transitionCounts that start on the
 * given state (i.e. the input key) or emissionCounts for the state and normalizing to
 * get new model parameters. 
 * @author dsalama
 *
 */
public class MaximizationReducer extends MapReduceBase implements
		Reducer<Text, ValueAndCount, EMModelParameter, DoubleWritable> {

	@Override
	public void reduce(Text state, Iterator<ValueAndCount> expectedCounts,
			OutputCollector<EMModelParameter, DoubleWritable> output, Reporter reporter) 
					throws IOException {
		double totalTransitionCount = 0.0;
		double totalEmissionCount = 0.0;
		Map<String, Double> emissionCounts = new HashMap<String, Double>();
		Map<String, Double> transitionCounts = new HashMap<String, Double>();
	
		// Accumulate counts and add to maps
		while (expectedCounts.hasNext()) {
			ValueAndCount vac = expectedCounts.next();
			switch (vac.getType()){
			case ValueAndCount.PARAMETER_TYPE_EMISSION:
				totalEmissionCount += vac.getCount();
				emissionCounts.put(vac.getValue(), vac.getCount());
				break;
			case ValueAndCount.PARAMETER_TYPE_TRANSITION:
				totalTransitionCount += vac.getCount();
				transitionCounts.put(vac.getValue(), vac.getCount());
				break;
			}
		}
		
		String sourceState = state.toString();
		
		// Output param with normalized prob for each emission
		for (Entry<String, Double> emission : emissionCounts.entrySet()) {
			double probability = emission.getValue() / totalEmissionCount;
			EMModelParameter param = EMModelParameter.emissionParameter(sourceState, 
					emission.getKey());
			output.collect(param, new DoubleWritable(probability));
		}
		
		// Output param with normalized prob for each transition
		for (Entry<String, Double> transition : transitionCounts.entrySet()) {
			double probability = transition.getValue() / totalTransitionCount;
			EMModelParameter param = EMModelParameter.transitionParameter(sourceState, 
					transition.getKey());
			output.collect(param, new DoubleWritable(probability));
		}
	}

}

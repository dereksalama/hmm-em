package edu.dartmouth.hmmem;

import java.io.DataInput;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3native.NativeS3FileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ExpectationMapper extends MapReduceBase implements
Mapper<LongWritable, Text, Text, EMModelParameter> {

	private static final Logger LOGGER = Logger.getLogger(ExpectationMapper.class.toString());

	public static final String BUCKET_URI_KEY = "bucket_uri";
	public static final String MODEL_PARAMETERS_DIR_PATH_KEY = "model_parameters_file_path";

	private final Map<StringPair, Double> transLogProbMap = new HashMap<StringPair, Double>();
	private final Map<StringPair, Double> emisLogProbMap = new HashMap<StringPair, Double>();

	private boolean failure = false;
	private String failureString;

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, EMModelParameter> output, Reporter reporter)
					throws IOException {
		LOGGER.log(Level.INFO, "ExpectationMapper");

		System.err.println("~~~~~~~~~~~~~ExpectationMapper~~~~~~~~~~~~~");

		if (failure) {
			System.err.print(failureString);
			output.collect(new Text(failureString), new EMModelParameter(EMModelParameter.PARAMETER_TYPE_TRANSITION, new Text(failureString), new Text(failureString), 1337.2));
			return;
		}

		// Test: Print out the contents of the maps to stderr and output collector.
		System.err.println("Trans file:");
		for (Entry<StringPair, Double> entry : transLogProbMap.entrySet()) {
			System.err.println("\t" + entry.getKey() + ": " + entry.getValue());
			
			EMModelParameter transitionParam = new EMModelParameter(EMModelParameter.PARAMETER_TYPE_TRANSITION,
					new Text(entry.getKey().x), new Text(entry.getKey().y), entry.getValue());
			output.collect(new Text("Transition"), transitionParam);
		}

		System.err.println("Emis file:");
		for (Entry<StringPair, Double> entry : emisLogProbMap.entrySet()) {
			System.err.println("\t" + entry.getKey() + ": " + entry.getValue());
			
			EMModelParameter emissionParam = new EMModelParameter(EMModelParameter.PARAMETER_TYPE_EMISSION,
					new Text(entry.getKey().x), new Text(entry.getKey().y), entry.getValue());
			output.collect(new Text("Transition"), emissionParam);
		}
	}

	/**
	 * Runs before each map. Obtains the path to the model parameters file from the job conf. Then
	 * parses the file and fills in the transition and emission log probabilities maps.
	 */
	@Override
	public void configure(JobConf job) {
		try {
			LOGGER.log(Level.INFO, "Configure");

			System.err.println("~~~~~~~~~~~~~Configure~~~~~~~~~~~~~");

			URI bucketURI;
			bucketURI = new URI(job.get(BUCKET_URI_KEY));

			FileSystem fs;
			fs = NativeS3FileSystem.get(bucketURI, new Configuration());

			Path modelParametersDirPath = new Path(job.get(MODEL_PARAMETERS_DIR_PATH_KEY));
			FileStatus[] modelParameterFileStatuses;
			modelParameterFileStatuses = fs.listStatus(modelParametersDirPath);


			for (FileStatus modelParameterFileStatus : modelParameterFileStatuses) {
				LOGGER.log(Level.INFO, "Parsing model parameters file: " + modelParameterFileStatus.getPath());

				DataInput modelParametersIn;
				modelParametersIn = fs.open(modelParameterFileStatus.getPath());
				readModelParametersFile(modelParametersIn);

			}

			LOGGER.log(Level.INFO, "End of configure()");
		} catch (Exception e) {
			failure = true;
			failureString = e.toString();

			LOGGER.log(Level.SEVERE, failureString);
		}
	}

	/**
	 * Reads the given model parameters file and fills in the transition and emission
	 * log probabilities maps.
	 */
	private void readModelParametersFile(DataInput in) throws IOException {
		LOGGER.log(Level.INFO, "Reading model parameters file.");

		EMModelParameter param;
		while (null != (param = EMModelParameter.read(in))) {
			switch (param.getParameterType()) {
			case EMModelParameter.PARAMETER_TYPE_TRANSITION:
				LOGGER.log(Level.INFO, "Transition: " + param.toString());

				StringPair fromStateToState =
						new StringPair(param.getTransFromStateOrEmisState().toString(),
								param.getTransToStateOrEmisToken().toString());

				if (transLogProbMap.containsKey(fromStateToState)) {
					failure = true;
					failureString = "Transition " + fromStateToState + " was read twice in EM model parameter files.";

					LOGGER.log(Level.SEVERE, failureString);

					return;
				}

				transLogProbMap.put(fromStateToState, param.getLogCount());
				break;
			case EMModelParameter.PARAMETER_TYPE_EMISSION:
				LOGGER.log(Level.INFO, "Emission: " + param.toString());

				StringPair stateToken =
						new StringPair(param.getTransFromStateOrEmisState().toString(),
								param.getTransToStateOrEmisToken().toString());

				if (emisLogProbMap.containsKey(stateToken)) {
					failure = true;
					failureString = "Emission " + stateToken + " was read twice in EM model parameter files.";

					LOGGER.log(Level.SEVERE, failureString);

					return;
				}

				emisLogProbMap.put(stateToken, param.getLogCount());
				break;
			}
		}
	}
}

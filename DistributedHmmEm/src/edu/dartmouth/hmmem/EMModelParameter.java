package edu.dartmouth.hmmem;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

/*
 * Represents a single EM model parameter, such as the probability of a transition or an emission.
 */
public class EMModelParameter implements Writable {
	
	public static final char PARAMETER_TYPE_TRANSITION = 't';
	public static final char PARAMETER_TYPE_EMISSION = 'e';
	
	// Indicates the type of the parameter:
	// 't' --> transition
	// 'e' --> emission
	private char parameterType = '\0';
	
	private Text transFromStateOrEmisState = new Text();
	private Text transToStateOrEmisToken = new Text();
	
	private double logCount = 0;
	
	public EMModelParameter() {}

	public EMModelParameter(char parameterType, Text transFromStateOrEmisState, Text transToStateOrEmisToken, double logCount) {
		this.parameterType = parameterType;
		this.transFromStateOrEmisState = transFromStateOrEmisState;
		this.transToStateOrEmisToken = transToStateOrEmisToken;
		this.logCount = logCount;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		parameterType = in.readChar();
		
		transFromStateOrEmisState.readFields(in);
		transToStateOrEmisToken.readFields(in);
		
		logCount = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeChar(parameterType);
		
		transFromStateOrEmisState.write(out);
		transToStateOrEmisToken.write(out);
		
		out.writeDouble(logCount);		
	}

	public char getParameterType() {
		return parameterType;
	}

	public void setParameterType(char parameterType) {
		this.parameterType = parameterType;
	}

	public Text getTransFromStateOrEmisState() {
		return transFromStateOrEmisState;
	}

	public void setTransFromStateOrEmisState(Text transFromStateOrEmisState) {
		this.transFromStateOrEmisState = transFromStateOrEmisState;
	}

	public Text getTransToStateOrEmisToken() {
		return transToStateOrEmisToken;
	}

	public void setTransToStateOrEmisToken(Text transToStateOrEmisToken) {
		this.transToStateOrEmisToken = transToStateOrEmisToken;
	}

	public double getLogCount() {
		return logCount;
	}

	public void setLogCount(double logCount) {
		this.logCount = logCount;
	}
}

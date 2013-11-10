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
	private char parameterType;
	
	// If a transition...
	private String transFromState = null;
	private String transToState = null;
	
	// If an emission...
	private String emisState = null;
	private String emisToken = null;
	
	// Default constructor with no parameters.
	public EMModelParameter() {
		parameterType = '\0';
	}
	
	public EMModelParameter(char parameterType, String transFromState, String transToState, String emisState, String emisToken) {
		super();
		
		this.parameterType = parameterType;
		
		switch (parameterType) {
			case PARAMETER_TYPE_TRANSITION:
				assert(emisState == null && emisToken == null);
				
				this.transFromState = transFromState;
				this.transToState = transToState;
				
				this.emisState = null;
				this.emisToken = null;
				
				break;
			case PARAMETER_TYPE_EMISSION:
				assert(transToState == null && transFromState == null);
				this.emisState = emisState;
				this.emisToken = emisToken;
				
				this.transFromState = null;
				this.transToState = null;
				
				break;
			default:
				assert(false); // Fail.
		}
	}
	
	public static EMModelParameter transitionParameter(String transFromState, String transToState) {
		return new EMModelParameter(PARAMETER_TYPE_TRANSITION, transFromState, transToState, null, null);
	}
	
	public static EMModelParameter emissionParameter(String emisState, String emisToken) {
		return new EMModelParameter(PARAMETER_TYPE_EMISSION, null, null, emisState, emisToken);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		parameterType = in.readChar();
		
		switch (parameterType) {
			case PARAMETER_TYPE_TRANSITION:
				Text fromStateText = new Text();
				fromStateText.readFields(in);
				transFromState = fromStateText.toString();
				
				Text toStateText = new Text();
				toStateText.readFields(in);
				transToState = toStateText.toString();
				
				break;
			case PARAMETER_TYPE_EMISSION:
				Text emisStateText = new Text();
				emisStateText.readFields(in);
				emisState = emisStateText.toString();
				
				Text emisTokenText = new Text();
				emisTokenText.readFields(in);
				emisToken = emisTokenText.toString();

				break;
			default:
				assert(false); // Fail.
		}
		
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeChar(parameterType);
		
		switch (parameterType) {
			case PARAMETER_TYPE_TRANSITION:
				Text fromStateText = new Text(transFromState);
				fromStateText.write(out);
				
				Text toStateText = new Text(transToState);
				toStateText.write(out);
				
				break;
			case PARAMETER_TYPE_EMISSION:
				Text emisStateText = new Text(emisState);
				emisStateText.write(out);
				
				Text emisTokenText = new Text(emisToken);
				emisTokenText.write(out);
				
				break;
			default:
				assert(false); // Fail.
		}
		
	}
	
}

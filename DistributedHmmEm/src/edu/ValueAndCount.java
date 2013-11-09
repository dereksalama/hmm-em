package edu;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

public class ValueAndCount implements Writable {
	
	public static final char PARAMETER_TYPE_TRANSITION = 't';
	public static final char PARAMETER_TYPE_EMISSION = 'e';
	
	// Indicates the type of the parameter:
	// 't' --> transition
	// 'e' --> emission
	private char type;
	
	private String value; // emission or state
	private double count; // expected count
	
	public char getType() {
		return type;
	}

	public String getValue() {
		return value;
	}

	public double getCount() {
		return count;
	}

	public ValueAndCount() {
		type = '\0';
		value = null;
		count = 0.0;
	}
	
	public ValueAndCount(char type, String value, double count) {
		this.type = type;
		this.value = value;
		this.count = count;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		type = in.readChar();
		value = in.readUTF();
		count = in.readDouble();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeChar(type);
		out.writeChars(value);
		out.writeDouble(count);
	}
	
	public static ValueAndCount read(DataInput in) throws IOException {
		ValueAndCount v = new ValueAndCount();
		v.readFields(in);
		return v;
	}

}

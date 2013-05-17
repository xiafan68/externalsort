package datasource;

import java.io.BufferedReader;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;


public class SimpleStringRecord implements IRecord{
	private String value = null;
	private String delimiter = null;
	
	public SimpleStringRecord() {
		this(null, " ");
	}
	
	public SimpleStringRecord(String value) {
		this(value, " ");
	}
	
	public SimpleStringRecord(String value, String delimiter) {
		this.value = value;
		this.delimiter = delimiter;
	}
	
	@Override
	public Comparable getRecordKey() {
		if (value == null)
			return null;
		String[] fields = value.split(delimiter);
		return Long.parseLong(fields[0].trim());
	}
	
	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	@Override
	public void readField(DataInput input) throws IOException {
		value = input.readLine();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(value);
		output.writeUTF("\n");
	}

	/**
	 * not implemented
	 */
	@Override
	public void readField(Reader reader) {
		try {
			value = ((BufferedReader)reader).readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void write(Writer writer) throws IOException {
		writer.write(value);
		writer.write("\n");
	}

	@Override
	public String toString() {
		return value;
	}
}

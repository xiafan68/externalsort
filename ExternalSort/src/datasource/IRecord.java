package datasource;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;

public interface IRecord {
	/**
	 * the key of the record or null if the record is not initialized
	 * 
	 * @return
	 */
	public Comparable getRecordKey();

	public void readField(DataInput input) throws IOException;

	public void write(DataOutput output) throws IOException;

	public void readField(Reader reader) throws IOException;

	public void write(Writer writer) throws IOException;
}

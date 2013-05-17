package datasource;

public class SimpleStringRecordFactory implements IRecordFactory{
	public static SimpleStringRecordFactory instance = new SimpleStringRecordFactory();
	
	@Override
	public IRecord getIRecord() {
		return new SimpleStringRecord();
	}

}

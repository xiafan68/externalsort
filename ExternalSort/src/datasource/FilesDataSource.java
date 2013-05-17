package datasource;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class FilesDataSource implements IDataSource {
	protected Iterator<File> fileIter = null;
	BufferedReader reader = null;
	IRecord cur = null;
	private IRecordFactory recordFactory = null;

	public FilesDataSource(IRecordFactory recordFactory) {
		this.recordFactory = recordFactory;
	}

	public FilesDataSource(File[] files, IRecordFactory recordFactory) {
		List<File> fileList = Arrays.asList(files);
		fileIter = fileList.iterator();
		this.recordFactory = recordFactory;
	}

	@Override
	public boolean hasNext() {
		if (cur != null)
			return true;

		try {
			cur = recordFactory.getIRecord();

			if (reader != null)
				cur.readField(reader);
			while (reader == null || null == cur.getRecordKey()) {
				if (fileIter.hasNext()) {
					File tmp = fileIter.next();
					if (reader != null)
						reader.close();
					reader = new BufferedReader(new FileReader(tmp));
				} else
					break;
				cur.readField(reader);
			}

			if (cur.getRecordKey() == null)
				cur = null;
		} catch (IOException e) {
			cur = null;
			e.printStackTrace();
		}
		return cur != null;
	}

	@Override
	public IRecord next() {
		if (hasNext()) {
			IRecord ret = cur;
			cur = null;
			return ret;
		} else
			return null;
	}

	@Override
	public void close() throws IOException {
		if (reader != null)
			reader.close();
	}
}

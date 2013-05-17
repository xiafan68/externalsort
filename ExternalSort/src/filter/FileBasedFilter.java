package filter;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

import datasource.IDataSource;
import datasource.IRecord;

/**
 * 这个类接受两个数据源，以其中一个数据的数据源作为过滤条件，过滤另一个文件中的数据 必须保证数据在两个文件中出现的次序一致
 * 
 * @author xiafan
 * 
 */
public class FileBasedFilter {
	private IDataSource filteringDataSource = null;
	private IDataSource filteredDataSource = null;
	private String outputFile = null;

	public FileBasedFilter(IDataSource filteringDataSource,
			IDataSource filteredDataSource, String outputFile) {
		this.filteringDataSource = filteringDataSource;
		this.filteredDataSource = filteredDataSource;
		this.outputFile = outputFile;
	}

	public void filter() throws IOException {
		IRecord filteringData = null;
		IRecord filteredData = null;
		BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile));

		boolean finish = false;
		try {
			while (filteringDataSource.hasNext()
					&& (filteredData != null || filteredDataSource.hasNext())) {
				filteringData = filteringDataSource.next();

				/*
				 * 从另一个数据源中读取数据直到遇到一条和filteringData不同的数据
				 */
				while (filteredDataSource.hasNext() || filteredData != null) {
					if (filteredData == null && filteredDataSource.hasNext()) {
						filteredData = filteredDataSource.next();
					} else if (filteredData == null) {
						finish = true;// 需要过滤的数据源已经没有数据
						break;
					}

					int cmp = filteringData.getRecordKey().compareTo(
							filteredData.getRecordKey());
					if (cmp == 0) {
						filteredData.write(writer);
						filteredData = null;
					} else if (cmp < 0)
						break;
					else
						filteredData = null;
				}

				if (finish)
					break;
			}
		} finally {
			if (writer != null)
				writer.close();
		}
	}

}

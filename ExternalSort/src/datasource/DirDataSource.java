package datasource;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * 这个类按行读取一个目录下面的所有文件
 * 
 * @author xiafan
 * 
 */
public class DirDataSource extends FilesDataSource {
	public DirDataSource(String dir, IRecordFactory recordFactory) {
		super(recordFactory);
		
		File file = new File(dir);
		List<File> files = null;
		if (file.exists() && file.isDirectory()) {
			files = Arrays.asList(file.listFiles(new FileFilter() {
				/**
				 * filter the directory and hidden files
				 */
				@Override
				public boolean accept(File file) {
					if (file.isDirectory()) {
						return false;
					}

					if (file.getName().startsWith(".")) {
						return false;
					}
					return true;
				}

			}));
			fileIter = files.iterator();
		}
	}
}

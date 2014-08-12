package output;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;

/**
 * 用户可以指定输出文件的大小，当文件大小超过设定的范围是，本实现将自动在指定目录下面创建新的输出文件
 * 
 * @author xiafan
 * 
 */
public class AutoSwitchWriter extends Writer {
	private BufferedWriter writer = null;
	private File file = null;
	private String outputDir = null;
	private int limit = 0;
	private int i = 0;

	/**
	 * 
	 * @param outputDir_
	 *            输出目录，所有的输出文件都将创建在该目录下面
	 * @param limit_
	 */
	public AutoSwitchWriter(String outputDir_, int limit_) {
		outputDir = outputDir_;
		limit = limit_;
	}

	@Override
	public void close() throws IOException {
		if (writer != null)
			writer.close();

	}

	@Override
	public void flush() throws IOException {
		if (writer != null)
			writer.flush();
	}

	@Override
	public void write(char[] arg0, int arg1, int arg2) throws IOException {
		if (writer == null || file.length() > limit) {
			createNewFile();
		}
		writer.write(arg0, arg1, arg2);
	}

	private void createNewFile() throws FileNotFoundException {
		file = new File(outputDir, "" + (i++));
		writer = new BufferedWriter(new OutputStreamWriter(
				new FileOutputStream(file)));
	}
}

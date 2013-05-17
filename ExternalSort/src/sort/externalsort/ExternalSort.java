package sort.externalsort;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import output.AutoSwithWriter;

import datasource.DirDataSource;
import datasource.FilesDataSource;
import datasource.IDataSource;
import datasource.IRecord;
import datasource.IRecordFactory;
import datasource.SimpleStringRecordFactory;

public class ExternalSort {
	public static int mergeStep = 50;
	public static File tmpDir = null;
	public static String tmpFilePrefix = "externalsort";
	public static String tmpFileDelimeter = ".";
	public static String delimeter = " ";
	private IRecordFactory recordFactory = SimpleStringRecordFactory.instance;

	static {
		String tmpDirStr = System.getProperty("tmpDir", "/tmp");
		tmpDir = new File(tmpDirStr);
	}

	IDataSource source = null;
	BufferedWriter writer = null;
	File outputFile = null;
	int recordsPerFile;
	IMemorySort sort = new MemorySort();

	public static Comparator<IRecord> comp = new Comparator<IRecord>() {
		@Override
		public int compare(IRecord arg0, IRecord arg1) {
			return arg0.getRecordKey().compareTo(arg1.getRecordKey());
		}

	};
	public static Comparator<IRecord> comparator = comp;

	int iteration = 0;

	/**
	 * 
	 * @param inputDir
	 *            原始数据存放目录
	 * @param outFile
	 *            输出数据存放目录
	 * @param recordsPerFile
	 *            初始文件分割阶段每个文件包含的记录个数
	 */
	public ExternalSort(String inputDir, String outFile, int recordsPerFile) {
		this.recordsPerFile = recordsPerFile;
		this.source = new DirDataSource(inputDir, recordFactory);
		outputFile = new File(outFile);
		this.writer = new BufferedWriter(new AutoSwithWriter(outFile, 1024*1024*64));
	}

	/**
	 * 归并排序
	 */
	public void sort() throws IOException {
		split();
		while (!merge()) {
			System.out.println("iteration " + iteration + " completes");
		}
	}

	/**
	 * 从原数据文件中读取数据，按照某个阈值将所有数据分割成多个文件，其中每个文件都进行了排序
	 * 
	 * @throws IOException
	 */
	private void split() throws IOException {
		int count = 0;
		List<IRecord> contents = new LinkedList<IRecord>();

		int fileCount = 0;
		File tmpFile = createTempFile(iteration, fileCount);
		BufferedWriter tmpOutput = new BufferedWriter(new FileWriter(tmpFile));
		while (source.hasNext()) {
			contents.add(source.next());
			count++;
			if (count >= recordsPerFile) {
				count = 0;
				// first sort in memory
				sort.sort(contents, comparator);

				// output the sorted string into temporary file
				for (IRecord record : contents) {
					record.write(tmpOutput);
					// tmpOutput.write(string);
					// tmpOutput.newLine();
				}
				tmpOutput.close();

				contents = new LinkedList<IRecord>();
				// create new tmp file
				fileCount++;
				tmpFile = createTempFile(iteration, fileCount);
				tmpOutput = new BufferedWriter(new FileWriter(tmpFile));
			}
		}

		if (contents.size() != 0) {
			sort.sort(contents, comparator);
			// output the sorted string into temporary file
			for (IRecord record : contents) {
				record.write(tmpOutput);
				// tmpOutput.write(string);
				// tmpOutput.newLine();
			}
			tmpOutput.close();
		}
		iteration++;
	}

	/**
	 * 
	 * @param iteration
	 * @return true if the external sort finishes
	 * @throws IOException
	 */
	public boolean merge() throws IOException {
		List<File> files = Arrays.asList(tmpDir.listFiles(new FileFilter() {
			/**
			 * filter the directory and hidden files
			 */
			@Override
			public boolean accept(File file) {
				if (file.isDirectory()) {
					return false;
				}

				if (file.getName().startsWith((iteration - 1) + tmpFilePrefix)) {
					return true;
				}
				return false;
			}
		}));

		/*
		 * 对排好序的中间临时文件进行归并
		 */
		int toIndex = 0;
		int count = 0;
		File tmpFile = null;
		BufferedWriter output = null;
		if (files.size() <= mergeStep) {
			output = writer;
		} else {
			tmpFile = createTempFile(iteration, count);
			output = new BufferedWriter(new FileWriter(tmpFile));
			count++;
		}
		for (int i = 0; toIndex < files.size(); i += mergeStep) {
			toIndex = (i + mergeStep) > files.size() ? files.size()
					: (i + mergeStep);

			merge(files.subList(i, toIndex), output);

			tmpFile = createTempFile(iteration, count);
			output = new BufferedWriter(new FileWriter(tmpFile));
			count++;
		}

		/*
		 * 删除之前归并步骤留下的临时文件
		 */
		if (iteration > 0) {
			for (int i = 0; i < count; i++) {
				removeFile(iteration - 1, i);
			}
		}

		iteration++;
		return files.size() <= mergeStep;
	}

	public void merge(List<File> files, BufferedWriter output)
			throws IOException {
		TreeMap<Comparable, List<Pair<IRecord, IDataSource>>> map = new TreeMap<Comparable, List<Pair<IRecord, IDataSource>>>();

		List<Pair<IRecord, IDataSource>> dataList = new LinkedList<Pair<IRecord, IDataSource>>();
		for (File file : files) {
			// BufferedReader reader = new BufferedReader(new FileReader(file));
			// String line = reader.readLine();
			IDataSource source = new FilesDataSource(new File[] { file },
					recordFactory);
			IRecord curRec = source.next();

			if (curRec == null)
				continue;
			else {
				Pair<IRecord, IDataSource> pair = new Pair<IRecord, IDataSource>(
						curRec, source);
				Comparable key = curRec.getRecordKey();
				if (map.get(key) == null) {
					List<Pair<IRecord, IDataSource>> list = new LinkedList<Pair<IRecord, IDataSource>>();
					list.add(pair);
					map.put(key, list);
				} else {
					map.get(key).add(pair);
				}
			}
		}

		// merge
		while (!map.isEmpty()) {
			Entry<Comparable, List<Pair<IRecord, IDataSource>>> entry = map
					.pollFirstEntry();

			for (Pair<IRecord, IDataSource> pair : entry.getValue()) {
				// output.write(pair.left);
				// output.newLine();
				pair.left.write(output);
				// fetch new data
				pair.left = pair.right.next();

				Comparable key = null;
				// write directly to the output file
				while (pair.left != null) {
					key = pair.left.getRecordKey();
					if (key.compareTo(entry.getKey()) == 0) {
						/*
						 * output.write(pair.left); output.newLine(); pair.left
						 * = pair.right.readLine();
						 */
						pair.left.write(output);
						pair.left = pair.right.next();
					} else {
						break;
					}
				}

				// add the to memory map
				if (pair.left != null) {
					// timeStamp = Long.parseLong(pair.left.substring(0,
					// pair.left.indexOf(delimeter)));
					if (map.get(key) == null) {
						List<Pair<IRecord, IDataSource>> list = new LinkedList<Pair<IRecord, IDataSource>>();
						list.add(pair);
						map.put(key, list);
					} else {
						map.get(key).add(pair);
					}
				} else {
					pair.right.close();
				}
			}
		}
		output.close();
	}

	public static class Pair<T1, T2> {
		public T1 left = null;
		public T2 right = null;

		public Pair(T1 a, T2 b) {
			left = a;
			right = b;
		}
	}

	private File createTempFile(int iteration, int fileCount)
			throws IOException {
		File file = File.createTempFile(String.valueOf(iteration)
				+ tmpFilePrefix, tmpFileDelimeter + String.valueOf(fileCount),
				tmpDir);
		file.deleteOnExit();
		return file;
	}

	private void removeFile(int iteration, int fileCount) throws IOException {
		File file = File.createTempFile(String.valueOf(iteration)
				+ tmpFilePrefix, tmpFileDelimeter + String.valueOf(fileCount),
				tmpDir);
		file.delete();
	}

	public IRecordFactory getRecordFactory() {
		return recordFactory;
	}

	public void setRecordFactory(IRecordFactory recordFactory) {
		this.recordFactory = recordFactory;
	}

	/**
	 * provide the inputdir, outputfile and maxlength
	 * 
	 * @param args
	 * @throws IOException
	 */
	public static void main(String[] args) throws IOException {
		args = new String[] { "./testData", "./outFile" };
		ExternalSort sort = new ExternalSort(args[0], args[1], 100000);
		sort.sort();
	}
}

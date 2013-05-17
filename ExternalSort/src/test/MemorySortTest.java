package test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;

import datasource.IRecord;
import datasource.SimpleStringRecord;

import sort.externalsort.ExternalSort;
import sort.externalsort.MemorySort;

public class MemorySortTest {
	@Test
	public void test() {
		List<IRecord> test = Arrays.asList(new IRecord[]{new SimpleStringRecord("12312 test"),
				new SimpleStringRecord("12313"),
				new SimpleStringRecord("22312 test"),
				new SimpleStringRecord("12314 test"),
				new SimpleStringRecord("12312 test"),
				new SimpleStringRecord("12311 test")});
		MemorySort sort = new MemorySort();
		sort.sort(test, ExternalSort.comp);
		System.out.println(test);
	}
}

package sort.externalsort;

import java.util.Comparator;
import java.util.List;

import datasource.IRecord;

public interface IMemorySort {
	public void sort(List<IRecord> contents, Comparator<IRecord> comp);
}

package sort.externalsort;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import datasource.IRecord;

public class MemorySort implements IMemorySort{

	@Override
	public void sort(List<IRecord> contents, Comparator<IRecord> comp) {
		Collections.sort(contents, comp);
	}

}

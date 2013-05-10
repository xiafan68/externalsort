package index;

import java.io.File;
import java.util.Comparator;

import org.junit.Test;

public class InvertedIndexTest {
	@Test
	public void test() {
		InvertedIndex<NodeIDPostElement> index = new InvertedIndex<NodeIDPostElement>(
				new File("indextest.inv"),
				NodeIDPostElement.binding,
				(Class<Comparator<byte[]>>) NodeIDPostElement.NodeComparator.class
						.asSubclass(Comparator.class));
		index.init();

		for (int i = 0; i < 10; i++)
			for (int j = i; j < 12; j++)
				index.put(Integer.toString(i), new NodeIDPostElement(j));

		String[] keywords = new String[] { "0", "3", "4", "5" };

		System.out.println(index.search(keywords).entrySet());
		/*
		 * for (Entry<String, List<NodeIDPostElement>> entry : index.search(
		 * keywords).entrySet()) { System.out.println(entry.getKey()); for
		 * (NodeIDPostElement ele : entry.getValue())
		 * System.out.print(ele.getId() + " "); System.out.println(); }
		 */

		index.close();
	}
}

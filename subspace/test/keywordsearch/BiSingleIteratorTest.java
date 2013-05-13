package keywordsearch;

import index.NodeIDPostElement;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import keywordsearch.bidirection.BiSingleIterator;
import keywordsearch.bidirection.BiSingleIterator.BiState;

import org.junit.Test;

import util.PerformanceTracker;
import xiafan.util.Triple;

public class BiSingleIteratorTest {
	@Test
	public void test() {
		/**
		 * 1 -1.0-> 2 -1.0-> 3 -1.0-> 4(10)
		 *            -1.0-> 5 -2.0-> 6(100)
		 *            6,5,2,1,4,3
		 *1 -1.0-> 2 -1.0-> 3 -1.0-> 4(10)
		 *            -1.0-> 5 -2.0-> 6(16)
		 *            6,4,5,3,2,1    cause 2 to be re evaluated       
		 *            
		 */
		Map<String, List<NodeIDPostElement>> postings = new HashMap<String, List<NodeIDPostElement>>();
		Map<Long, Float> presMap = new HashMap<Long, Float>();

		postings.put("test1", new LinkedList<NodeIDPostElement>());
		postings.get("test1").add(new NodeIDPostElement(4));
		presMap.put(4l, 10.0f);
		postings.put("test2", new LinkedList<NodeIDPostElement>());
		postings.get("test2").add(new NodeIDPostElement(6));
		presMap.put(6l, 16.0f);

		HashMap<Long, HashMap<Long, Float>> nodeFromNodeDist = new HashMap<Long, HashMap<Long, Float>>();
		for (int i = 0; i < 6; i++) {
			nodeFromNodeDist.put(i + 1l, new HashMap<Long, Float>());
		}
		nodeFromNodeDist.get(4l).put(3l, 1.0f);
		nodeFromNodeDist.get(3l).put(2l, 1.0f);
		nodeFromNodeDist.get(2l).put(1l, 1.0f);
		nodeFromNodeDist.get(6l).put(5l, 2.0f);
		nodeFromNodeDist.get(5l).put(2l, 1.0f);

		BiState state = new BiState(postings, presMap);
		BiSingleIterator iter = new BiSingleIterator(state);
		state.addPrestigeListner(iter);
		PerformanceTracker.instance.startExplore();
		iter.init();
		List seq = new LinkedList();
		while (iter.hasNext()) {
			Triple<Long, Integer, Float> cur = iter.next();
			seq.add(cur);
			System.out.println(cur);
			if (nodeFromNodeDist.containsKey(cur.arg0))
				for (Entry<Long, Float> entry : nodeFromNodeDist.get(cur.arg0)
						.entrySet()) {
					state.explore(entry.getKey(), cur.arg0, entry.getValue());
					iter.addNode(new Triple<Long, Integer, Float>(entry
							.getKey(), cur.arg1 + 1, cur.arg2 * 0.5f));
				}
		}
		System.out.println(seq);
		PerformanceTracker.instance.finishExplore();
		PerformanceTracker.instance.print();
	}
}

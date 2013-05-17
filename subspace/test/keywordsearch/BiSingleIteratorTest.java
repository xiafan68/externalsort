package keywordsearch;

import index.NodeIDPostElement;

import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Random;

import junit.framework.Assert;

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

	@Test
	public void prioQueueTest() {
		PriorityQueue<Triple<Long, Integer, Float>> queue = new PriorityQueue<Triple<Long, Integer, Float>>(
				10, new Comparator<Triple<Long, Integer, Float>>() {
					@Override
					public int compare(Triple<Long, Integer, Float> arg0,
							Triple<Long, Integer, Float> arg1) {
						if ((arg1.arg2 - arg0.arg2) > 0.0000000000001)
							return 1;
						else if ((arg1.arg2 - arg0.arg2) < -0.00000000001)
							return -1;
						return 0;
					}
				});
		Random rand = new Random();
		for (int i = 0; i < 1000; i++) {
			queue.add(new Triple<Long, Integer, Float>((long) i, 0, rand
					.nextFloat()));
		}

		Triple<Long, Integer, Float> pre = queue.poll();
		while (!queue.isEmpty()) {
			System.out.println(queue.peek() + " ");
			if (pre.arg2 < queue.peek().arg2) {
				System.out.println("illegal sequence:" + pre + " "
						+ queue.peek());
			}
			queue.poll();
		}

		/*
		 * multiple instances of the same value can exists in the priorityQueue
		 * the contains method could be used to judge whether the value exists
		 * remove will only delete one instance of the value
		 */

		Queue<Long> queue1 = new PriorityQueue<Long>();
		queue1.add(10l);
		if (!queue1.contains(10l))
			queue1.add(10l);
		queue1.add(11l);
		Assert.assertEquals(2, queue1.size());
		queue1.add(11l);
		queue1.remove(11l);
		Assert.assertEquals(2, queue1.size());
	}

	@Test
	public void linkedListQueueTest() {
		Queue<Long> queue = new LinkedList<Long>();

		/*
		 * multiple instances of the same value can exists in the priorityQueue
		 * the contains method could be used to judge whether the value exists
		 * remove will only delete one instance of the value
		 */

		queue.add(10l);
		if (!queue.contains(10l))
			queue.add(10l);
		queue.add(11l);
		Assert.assertEquals(2, queue.size());
		queue.add(11l);
		queue.remove(11l);
		Assert.assertEquals(2, queue.size());
	}
}

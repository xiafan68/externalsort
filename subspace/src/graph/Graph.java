package graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;

import xiafan.util.Pair;

/**
 * 如何设计一个graph 1. 能够实现内存与磁盘的交互。 2. 快速访问某条边的某个属性
 * 
 * @author xiafan
 * 
 */
public class Graph {
	HashMap<Long, HashSet<Long>> adjMatrix = new HashMap<Long, HashSet<Long>>();

	public void addEdge(long start, long end) {
		if (adjMatrix.containsKey(start)) {
			adjMatrix.get(start).add(end);
		} else {
			HashSet<Long> list = new HashSet<Long>();
			list.add(end);
			adjMatrix.put(start, list);
		}
	}

	public Iterator<Long> nodeIter() {
		return adjMatrix.keySet().iterator();
	}

	public Iterator<Long> getTargetNode(long source) {
		return adjMatrix.get(source).iterator();
	}

	public Iterator<Pair<Long, Long>> edgeIter() {
		return new Iterator<Pair<Long, Long>>() {
			Iterator<Entry<Long, HashSet<Long>>> iter = adjMatrix.entrySet()
					.iterator();
			Entry<Long, HashSet<Long>> curEntry = null;
			Iterator<Long> targetIter = null;

			@Override
			public boolean hasNext() {
				while (targetIter == null || !targetIter.hasNext()) {
					if (!iter.hasNext())
						break;
					curEntry = iter.next();
					targetIter = curEntry.getValue().iterator();
				}
				if (targetIter != null && targetIter.hasNext())
					return true;
				return false;
			}

			@Override
			public Pair<Long, Long> next() {
				if (hasNext()) {
					return new Pair<Long, Long>(curEntry.getKey(),
							targetIter.next());
				}
				return null;
			}

			@Override
			public void remove() {
			}
		};
	}

	@Override
	public String toString() {
		return adjMatrix.toString();
	}
}

package graph;

import java.util.HashMap;
import java.util.HashSet;

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

	@Override
	public String toString() {
		return adjMatrix.toString();
	}
}

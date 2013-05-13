package keywordsearch.bidirection;

import index.NodeIDPostElement;

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;

import util.PerformanceTracker;
import xiafan.util.Triple;

/**
 * 
 * 1. 记录每两个节点之间的距离
 * 2. 针对每个节点，记录它们的祖父及子孙
 * 3. 通过priorityqueue维护目前已经touched的节点，排序的方式根据的是priority.此外还需要能够快速定位一个node，便于更新它们的
 * @author xiafan
 *
 */
public class BiSingleIterator implements
		Iterator<Triple<Long, Integer, Float>>, NodeStateListener {
	BiState state = null;
	// nodeid, depth,weight
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
	HashMap<Long, Triple<Long, Integer, Float>> queueIndex = new HashMap<Long, Triple<Long, Integer, Float>>();
	HashSet<Long> visitedNode = new HashSet<Long>();

	public BiSingleIterator(BiState state) {
		this.state = state;
	}

	public void init() {
		for (long seed : state.getVisistedNode()) {
			addNode(new Triple<Long, Integer, Float>(seed, 0,
					state.getNodePres(seed)));
		}
	}

	@Override
	public boolean hasNext() {
		return !queue.isEmpty();
	}

	public Triple<Long, Integer, Float> poll() {
		return queue.peek();
	}

	@Override
	public Triple<Long, Integer, Float> next() {
		Triple<Long, Integer, Float> triple = queue.poll();
		queueIndex.remove(triple.arg0);
		PerformanceTracker.instance.incre(PerformanceTracker.TOUCHED, 1);
		return triple;
	}

	public void addNode(Triple<Long, Integer, Float> newNode) {
		if (!visitedNode.contains(newNode.arg0)) {
			PerformanceTracker.instance.incre(PerformanceTracker.EXPLORED, 1);
			visitedNode.add(newNode.arg0);
			queue.add(newNode);
			queueIndex.put(newNode.arg0, newNode);
		}
	}

	@Override
	public void remove() {
		throw new RuntimeException(
				"unimplemented methods remove of BiSingleIterator");
	}

	/**
	 * invoked by the BiState to update prestige of nodes in the queue
	 * @param node
	 * @param prestige
	 */
	public void notifyStateChange(long node) {
		if (!queueIndex.containsKey(node))
			return;
		Triple<Long, Integer, Float> triple = queueIndex.get(node);
		queue.remove(triple);

		triple.arg2 = 0.0f;
		for (float pres : state.getNodePresMap(triple.arg0).values())
			triple.arg2 += pres;
		queue.add(triple);
	}

	/**
	 * explore the edges of a node
	 * 1. update the distance of a node to a keyword
	 * 2. if this node has been explored before, update the priority and distance of its successor
	 * this require the state could return the successor of some node
	 * @author xiafan
	 *child -> cur -> ancestor
	 */
	public static class BiState {
		// the distant of a node to a keyword, initialize the map with results
		// from inverted index
		HashMap<Long, HashMap<String, Float>> distToKeyword = new HashMap<Long, HashMap<String, Float>>();
		HashMap<Long, HashMap<Long, Float>> nodeToNodeDist = new HashMap<Long, HashMap<Long, Float>>();
		// node prestige, record the prestige from each keyword
		HashMap<Long, HashMap<String, Float>> nodePrestige = new HashMap<Long, HashMap<String, Float>>();
		// the element represents the ancestor of the x dimension when going to
		// the
		// keyword
		HashMap<Long, HashMap<String, Long>> path = new HashMap<Long, HashMap<String, Long>>();
		// record the ancestor of the key node
		HashMap<Long, HashMap<String, Long>> reversePath = new HashMap<Long, HashMap<String, Long>>();

		public BiState(Map<String, List<NodeIDPostElement>> postLists,
				Map<Long, Float> nodePres) {
			for (Entry<String, List<NodeIDPostElement>> entry : postLists
					.entrySet()) {
				for (NodeIDPostElement ele : entry.getValue()) {
					setNodeToKeywordDist(ele.getId(), entry.getKey(),
							ele.getId(), 0.0f);
					// TODO supply the prostige
					setNodeFromKeywordPrestige(ele.getId(), entry.getKey(),
							nodePres.get(ele.getId()) / entry.getValue().size());
				}
			}
		}

		List<NodeStateListener> presListners = new LinkedList<NodeStateListener>();

		public void addPrestigeListner(NodeStateListener listener) {
			presListners.add(listener);
		}

		List<NodeStateListener> distListners = new LinkedList<NodeStateListener>();

		public void addDistListner(NodeStateListener listener) {
			distListners.add(listener);
		}

		public HashMap<String, Float> getHits(long node) {
			if (!distToKeyword.containsKey(node))
				return new HashMap<String, Float>();
			return distToKeyword.get(node);
		}

		public Set<Long> getVisistedNode() {
			return nodePrestige.keySet();
		}

		private static final HashMap<String, Long> EMPTY_CHILD = new HashMap<String, Long>();

		private HashMap<String, Long> getChildren(long nodeID) {
			if (reversePath.containsKey(nodeID))
				return reversePath.get(nodeID);
			else
				return EMPTY_CHILD;
		}

		public HashMap<String, Long> getAncestors(long node) {
			if (path.containsKey(node))
				return path.get(node);
			else
				return EMPTY_CHILD;
		}

		public Long getAncestor(long node, String keyword) {
			long ret = -1;
			if (path.containsKey(node) && path.get(node).containsKey(keyword)) {
				ret = path.get(node).get(keyword);
			}
			return ret;
		}

		public float getNodeToNodeDist(long start, long end) {
			if (start == end)
				return 0.0f;
			else
				return nodeToNodeDist.get(start).get(end);
		}

		/**
		 * setup the direct distance between the two nodes
		 * @param start
		 * @param end
		 * @param dist
		 */
		public void setNodeToNodeDist(long start, long end, float dist) {
			if (!nodeToNodeDist.containsKey(start)) {
				nodeToNodeDist.put(start, new HashMap<Long, Float>());
			}
			nodeToNodeDist.get(start).put(end, dist);
		}

		public HashMap<String, Float> getNodePresMap(long node) {
			return nodePrestige.get(node);
		}

		public float getNodePres(long node) {
			float ret = 0.0f;
			if (nodePrestige.containsKey(node)) {
				for (float pres : nodePrestige.get(node).values()) {
					ret += pres;
				}
			}
			return ret;
		}

		public float getNodeFromKeywordPrestige(long node, String keyword) {
			float ret = 0.0f;
			if (nodePrestige.containsKey(node)) {
				if (nodePrestige.get(node).containsKey(keyword))
					ret = nodePrestige.get(node).get(keyword);
			}
			return ret;
		}

		public void setNodeFromKeywordPrestige(long workingNode,
				String keyword, float prestige) {
			if (!nodePrestige.containsKey(workingNode)) {
				nodePrestige.put(workingNode, new HashMap<String, Float>());
			}
			nodePrestige.get(workingNode).put(keyword, prestige);
		}

		public float getNodeToKeywordDist(long node, String keyword) {
			if (!distToKeyword.containsKey(node)
					|| !distToKeyword.get(node).containsKey(keyword))
				return 100000.0f;
			else
				return distToKeyword.get(node).get(keyword);
		}

		/**
		 * the shortest dist from node to keyword via pre
		 * @param node
		 * @param keyword
		 * @param pre
		 * @param dist
		 */
		public void setNodeToKeywordDist(long node, String keyword, long pre,
				float dist) {
			if (!distToKeyword.containsKey(node)) {
				distToKeyword.put(node, new HashMap<String, Float>());
			}
			distToKeyword.get(node).put(keyword, dist);

			if (!path.containsKey(node)) {
				path.put(node, new HashMap<String, Long>());
			}
			path.get(node).put(keyword, pre);

			if (!reversePath.containsKey(pre)) {
				reversePath.put(pre, new HashMap<String, Long>());
			}
			reversePath.get(pre).put(keyword, node);
		}

		private void propDist(long cur, String keyword) {
			Queue<Long> seed = new LinkedList<Long>();
			seed.add(cur);
			while (!seed.isEmpty()) {
				long workingNode = seed.poll();
				float workingDist = getNodeToKeywordDist(workingNode, keyword);
				HashMap<String, Long> childs = getChildren(workingNode);
				if (childs.containsKey(keyword)) {
					// child -> workingNode
					// update dist from each keyword
					long child = childs.get(keyword);
					float newDist = getNodeToNodeDist(workingNode, child)
							+ workingDist;
					if (newDist < getNodeToKeywordDist(child, keyword)) {
						seed.add(child);
						setNodeToKeywordDist(child, keyword, workingNode,
								newDist);
						notifyDistChange(cur);
					}
				}
			}
		}

		private void activate(long cur, String keyword) {
			Queue<Long> seed = new LinkedList<Long>();
			seed.add(cur);
			while (!seed.isEmpty()) {
				long workingNode = seed.poll();
				float workingPres = getNodePres(workingNode);
				HashMap<String, Long> childs = getChildren(workingNode);
				if (childs.containsKey(keyword)) {
					// child -> workingNode
					// update dist from each keyword
					long child = childs.get(keyword);
					// setNodePres
					if (getNodeFromKeywordPrestige(child, keyword) < workingPres
							* STEP) {
						seed.add(child);
						setNodeFromKeywordPrestige(child, keyword, workingPres
								* STEP);
						notifyPresChange(child);
					}
				}
			}
		}

		/**
		 * cur is just explored by pre from keyword.
		 * possible actions:
		 * 1. update the distance of the successor of cur to the keyword
		 * 2. update the prestige of the successor of cur, performed by invoker of this function
		 * @param keyword
		 * @param cur
		 * @param pre
		 * @param dist the direct distance from cur to pre
		 * @param prestige
		 * @return true if the node has not been visited
		 * false if the node has been visited, which means prestige needed to propagated
		 */
		public void explore(long cur, long pre, float dist) {
			setNodeToNodeDist(pre, cur, dist);
			// set up the path
			for (Entry<String, Float> entry : getHits(pre).entrySet()) {
				float curDist = getNodeToKeywordDist(cur, entry.getKey());
				String keyword = entry.getKey();
				float newDist = entry.getValue() + dist;
				if (newDist < curDist) {
					setNodeToKeywordDist(cur, keyword, pre, newDist);
					notifyDistChange(cur);
					propDist(cur, keyword);
				}

				if (getNodeFromKeywordPrestige(cur, keyword) < getNodeFromKeywordPrestige(
						pre, keyword) * STEP) {
					setNodeFromKeywordPrestige(cur, keyword,
							getNodeFromKeywordPrestige(pre, keyword) * STEP);
					notifyPresChange(cur);
					activate(cur, keyword);
				}

			}
		}

		public void notifyPresChange(long node) {
			for (NodeStateListener listner : presListners)
				listner.notifyStateChange(node);
		}

		public void notifyDistChange(long node) {
			for (NodeStateListener listner : distListners)
				listner.notifyStateChange(node);
		}

		private static final float STEP = 0.5f;
	}

}

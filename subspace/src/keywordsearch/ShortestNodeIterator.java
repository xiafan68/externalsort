package keywordsearch;

import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.PriorityQueue;

import keywordsearch.Neo4jKeywordSearch.DjskState;

import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.kernel.Traversal;

import util.Constant;
import xiafan.util.Pair;
import xiafan.util.Triple;

/**
 * 每次选择priority queue中最前的节点进行扩展，然后返回最前面的节点。
 * 
 * @author xiafan
 * 
 */
public class ShortestNodeIterator implements Iterator<Pair<Long, Float>> {
	DjskState state;

	public long getSource() {
		return source;
	}

	public void setSource(long source) {
		this.source = source;
	}

	int maxDepth;
	long source;
	PriorityQueue<Triple<Long, Integer, Float>> queue = new PriorityQueue<Triple<Long, Integer, Float>>(
			10, new Comparator<Triple<Long, Integer, Float>>() {

				@Override
				public int compare(Triple<Long, Integer, Float> arg0,
						Triple<Long, Integer, Float> arg1) {
					return (int) (arg0.arg2 - arg1.arg2);
				}

			});
	GraphDatabaseService graphDb;
	RelationshipType rel;

	public ShortestNodeIterator(long source, GraphDatabaseService graphDb,
			DjskState state, RelationshipType rel, int maxDepth) {
		this.source = source;
		visitedNode.add(source);
		this.state = state;
		this.maxDepth = maxDepth;
		this.graphDb = graphDb;
		this.rel = rel;
		queue.add(new Triple<Long, Integer, Float>(source, 0, 0.0f));
	}

	@Override
	public boolean hasNext() {
		return poll() != null;
	}

	public Pair<Long, Float> poll() {
		if (cur == null) {
			cur = next();
		}
		return cur;
	}

	Pair<Long, Float> cur = null;

	HashSet<Long> visitedNode = new HashSet<Long>();

	@Override
	public Pair<Long, Float> next() {
		if (cur != null) {
			Pair<Long, Float> pre = cur;
			cur = null;
			return pre;
		} else if (queue.isEmpty())
			return null;

		Triple<Long, Integer, Float> pre = queue.poll();
		if (pre.arg1 < maxDepth) {
			Node node = graphDb.getNodeById(pre.arg0);
			Expander expander = null;
			if (rel != null)
				expander = Traversal.expanderForTypes(rel, Direction.OUTGOING);
			else {
				expander = Traversal.expanderForAllTypes(Direction.OUTGOING);
			}

			Iterator<Relationship> iter = expander.expand(node).iterator();
			while (iter.hasNext()) {
				Relationship rel = iter.next();
				/*
				 * TODO 这里错了，应该是distance(source, rel.getEndNode.getId())
				 * 另外因为是shortest path，应该判断当前距离和distance对比。
				 * 
				 * 可不可能两个节点已经被访问过，然后再次以更短的距离访问。
				 */
				float preDist = state.distance(node.getId(), rel.getEndNode()
						.getId());
				if (!visitedNode.contains(rel.getEndNode().getId())) {
					float curRelWeight = (Float) rel
							.getProperty(Constant.WEIGHT);

					visitedNode.add(rel.getEndNode().getId());
					if (preDist > pre.arg2 + curRelWeight) {
						// 当前的距离更短，应该是有问题的
						queue.add(new Triple<Long, Integer, Float>(rel
								.getEndNode().getId(), pre.arg1 + 1, pre.arg2
								+ curRelWeight));
						state.addDistance(source, rel.getEndNode().getId(),
								node.getId(), pre.arg2 + curRelWeight);
					}
				}
			}
		}
		if (queue.isEmpty())
			return null;
		return new Pair<Long, Float>(queue.peek().arg0, queue.peek().arg2);
	}

	@Override
	public void remove() {
		throw new RuntimeException("unimplemented");
	}

}
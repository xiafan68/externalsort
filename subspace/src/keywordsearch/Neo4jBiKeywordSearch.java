package keywordsearch;

import graph.Graph;
import graph.KeywordSearch;
import graph.MyRelationshipTypes;
import index.ComposeKeyBtree;
import index.InvertedIndex;
import index.NodeIDPostElement;

import java.io.File;
import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import keywordsearch.BiSingleIterator.BiState;

import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Expander;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.kernel.Traversal;

import util.Constant;
import util.PerformanceTracker;
import xiafan.util.Pair;
import xiafan.util.Triple;

/**
 * 和backward search不同，内部状态维护的是点与点之间的最近距离，bidirectional search维护的是点到keyword之间的距离
 * TODO: 1. 计算每个节点的权重，怎么算？每个keywords在每个doc里面可以算一个权重，那么每个node对应的权重是什么呢？ 
 * TODO: 2. 如何计算每棵树的权重？
 * TODO: 3. 如何设计iterator，进行图上的遍历 
 * @author xiafan
 * 
 */
public class Neo4jBiKeywordSearch implements KeywordSearch {
	GraphDatabaseService graphDb;
	InvertedIndex<NodeIDPostElement> textIndex;
	ComposeKeyBtree<NodeIDPostElement> keyIndex;
	ExecutionEngine engine;

	PriorityQueue<Pair<Long, Float>> incomingQueue = new PriorityQueue<Pair<Long, Float>>();
	PriorityQueue<Pair<Long, Float>> outcomingQueue = new PriorityQueue<Pair<Long, Float>>();

	public static final float ANEAL = 0.5f;

	public Neo4jBiKeywordSearch(String path) {
		graphDb = new EmbeddedGraphDatabase(path);
		Map<String, String> config = new HashMap<String, String>();
		config.put("neostore.nodestore.db.mapped_memory", "100M");
		config.put("neostore.relationshipstore.db.mapped_memory", "200M");
		config.put("neostore.propertystore.db.mapped_memory", "200M");
		config.put("neostore.propertystore.db.strings.mapped_memory", "400M");
		config.put("neostore.propertystore.db.arrays.mapped_memory", "200M");
		config.put("keep_logical_logs", "300M size");

		graphDb = new EmbeddedGraphDatabase(path, config);
		textIndex = new InvertedIndex<NodeIDPostElement>(
				new File(path, "textindex"),
				NodeIDPostElement.binding,
				(Class<Comparator<byte[]>>) NodeIDPostElement.NodeComparator.class
						.asSubclass(Comparator.class));
		textIndex.init();

		keyIndex = new ComposeKeyBtree(path + "/keyIndex", 1024 * 1024 * 512);
		keyIndex.init(
				NodeIDPostElement.binding,
				(Class<Comparator<byte[]>>) NodeIDPostElement.NodeComparator.class
						.asSubclass(Comparator.class));
	}

	@Override
	public Node getNode(String key) {
		List<NodeIDPostElement> hits = keyIndex.get(key);
		if (!hits.isEmpty()) {
			return graphDb.getNodeById(hits.get(0).getId());
		} else
			return null;
	}

	@Override
	public List<Node> getNodes(String key) {
		List<Node> ret = new LinkedList<Node>();
		List<NodeIDPostElement> hits = keyIndex.get(key);
		for (NodeIDPostElement elem : hits) {
			ret.add(graphDb.getNodeById(elem.getId()));
		}
		return ret;
	}

	@Override
	public List<Graph> search(String field, String[] keywords) {
		return search(field, keywords, 10);
	}

	@Override
	public List<Graph> search(String field, String keywords, int topK)
			throws IOException {
		PerformanceTracker.instance.startExplore();
		Map<String, List<NodeIDPostElement>> results = textIndex
				.search(keywords);
		List<Graph> ret = searchIntern(results, topK);
		PerformanceTracker.instance.finishExplore();
		return ret;
	}

	@Override
	public List<Graph> search(String field, String[] keywords, int topK) {
		PerformanceTracker.instance.startExplore();
		Map<String, List<NodeIDPostElement>> results = textIndex
				.search(keywords);
		List<Graph> ret = searchIntern(results, topK);
		PerformanceTracker.instance.finishExplore();
		return ret;
	}

	private static final int maxDepth = 10;

	private List<Graph> searchIntern(
			Map<String, List<NodeIDPostElement>> results, int topK) {
		List<Graph> ret = null;
		BiState state = new BiState(results);
		BiSingleIterator outIter = new BiSingleIterator(state);
		BiSingleIterator inIter = new BiSingleIterator(state);
		inIter.init();

		HashSet<Long> roots = new HashSet<Long>();
		while (outIter.hasNext() || inIter.hasNext()) {
			Direction dir = null;
			Triple<Long, Integer, Float> triple = null;

			if (outIter.hasNext() && inIter.hasNext()) {
				if (outIter.poll().arg2 > inIter.poll().arg2) {
					triple = outIter.next();
					dir = Direction.OUTGOING;
				} else {
					triple = inIter.next();
					dir = Direction.INCOMING;
					outIter.addNode(triple);
				}
			} else if (outIter.hasNext()) {
				dir = Direction.OUTGOING;
				triple = outIter.next();
			} else {
				dir = Direction.INCOMING;
				triple = inIter.next();
			}

			if (state.getAncestors(triple.arg0).size() == results.size()) {
				roots.add(triple.arg0);
				if (roots.size() > topK) {
					// TODO just break now?
					break;
				}
			}

			// explore the neighbors
			if (triple.arg1 < maxDepth) {
				Node node = graphDb.getNodeById(triple.arg0);
				Expander expander = null;
				expander = Traversal.expanderForAllTypes(dir);
				Iterator<Relationship> iter = expander.expand(node).iterator();
				while (iter.hasNext()) {
					Relationship rel = iter.next();
					Node newNode = rel.getEndNode();
					float dist = (Float) rel.getProperty(Constant.WEIGHT);
					if (dir == Direction.INCOMING) {
						state.explore(newNode.getId(), triple.arg0, dist);
						inIter.addNode(new Triple<Long, Integer, Float>(newNode
								.getId(), triple.arg1 + 1, state
								.getNodePres(newNode.getId())));
					} else {
						state.explore(triple.arg0, newNode.getId(), dist);
						outIter.addNode(new Triple<Long, Integer, Float>(
								newNode.getId(), triple.arg1 + 1, state
										.getNodePres(newNode.getId())));
					}
				}
			}
		}
		ret = constructGraphs(state, roots);
		return ret;
	}

	private List<Graph> constructGraphs(BiState state, HashSet<Long> result) {
		List<Graph> graphes = new LinkedList<Graph>();
		for (long root : result) {
			HashMap<String, Long> sources = state.getAncestors(root);
			Graph graph = new Graph();
			for (Entry<String, Long> source : sources.entrySet()) {
				long temp = root;
				long pre = source.getValue();
				do {
					graph.addEdge(temp, pre);
					temp = pre;
					pre = state.getAncestor(temp, source.getKey());
				} while (pre != temp);
			}
			graphes.add(graph);
		}
		return graphes;
	}

	@Override
	public void addEdge(Node start, Node end, float weight) {
		Transaction tnx = graphDb.beginTx();
		Relationship ship = start.createRelationshipTo(end,
				MyRelationshipTypes.KNOWS);
		ship.setProperty(Constant.WEIGHT, weight);
		tnx.success();
		tnx.finish();
	}

	@Override
	public Node addNode(String key, boolean indexKey,
			Map<String, String> property, String indexField) {
		Transaction tnx = graphDb.beginTx();
		Node node = null;
		try {
			node = graphDb.createNode();
			node.setProperty(Constant.KEY, key);
			if (property != null) {
				for (Entry<String, String> entry : property.entrySet()) {
					node.setProperty(entry.getKey(), entry.getValue());
				}
				if (indexField != null) {
					// nodeIndex.add(node, indexField,
					// node.getProperty(indexField));

					textIndex.put((String) node.getProperty(indexField),
							new NodeIDPostElement(node.getId()));

				}
			}
			textIndex.put(key, new NodeIDPostElement(node.getId()));
			// nodeIndex.add(node, Constant.KEY, key);
			if (indexKey)
				keyIndex.put(key, new NodeIDPostElement(node.getId()));
			tnx.success();
		} catch (IOException e) {
			tnx.failure();
			node = null;
		} finally {
			tnx.finish();
		}
		return node;
	}

	int opCount = 0;

	// TODO add the part of caculate the node prestige
	@Override
	public void addSubgraph(
			List<Pair<Pair<String, Boolean>, Pair<String, Boolean>>> edges,
			List<Float> weights) throws IOException {
		Transaction tnx = graphDb.beginTx();
		Node startNode = null;
		Node endNode = null;
		Iterator<Float> iter = weights.iterator();
		for (Pair<Pair<String, Boolean>, Pair<String, Boolean>> entry : edges) {
			startNode = this.getNode(entry.arg0.arg0);
			if (startNode == null) {
				startNode = graphDb.createNode();
				startNode.setProperty(Constant.KEY, entry.arg0.arg0);
				// nodeIndex.add(startNode, Constant.KEY, entry.arg0.arg0);
				textIndex.put(entry.arg0.arg0,
						new NodeIDPostElement(startNode.getId()));
				if (entry.arg0.arg1)
					keyIndex.put(entry.arg0.arg0, new NodeIDPostElement(
							startNode.getId()));
			}

			endNode = this.getNode(entry.arg1.arg0);
			if (endNode == null) {
				endNode = graphDb.createNode();
				endNode.setProperty(Constant.KEY, entry.arg1.arg0);
				textIndex.put(entry.arg1.arg0,
						new NodeIDPostElement(endNode.getId()));
				if (entry.arg0.arg1)
					keyIndex.put(entry.arg1.arg0,
							new NodeIDPostElement(endNode.getId()));
			}

			float weight = iter.next();
			Relationship ship = startNode.createRelationshipTo(endNode,
					MyRelationshipTypes.KNOWS);
			ship.setProperty(Constant.WEIGHT, weight);
		}
		tnx.success();
		tnx.finish();
		if (opCount++ % 1000 == 0) {
			textIndex.flush();
			keyIndex.flush();
		}
	}

	@Override
	public Node getNodeById(long id) {
		return graphDb.getNodeById(id);
	}

	@Override
	public void close() {
		graphDb.shutdown();
		keyIndex.close();
		textIndex.close();
	}

	public void printPerform() {
		System.out.println(String.format(
				"btree: recent put:%f;\nrecent retrieve:%f\n",
				keyIndex.getRecentPutLatency(),
				keyIndex.getRecentRetrievelLatency()));
		textIndex.printPerformance();
	}
}

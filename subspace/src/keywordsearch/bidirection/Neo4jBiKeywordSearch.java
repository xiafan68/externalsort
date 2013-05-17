package keywordsearch.bidirection;

import graph.Graph;
import graph.KeywordSearch;
import graph.MyRelationshipTypes;
import index.ComposeKeyBtree;
import index.CompressedInvIndex;
import index.IInvertedIndex;
import index.InvertedIndex;
import index.NodeIDPostElement;
import index.PresElement;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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

import keywordsearch.bidirection.BiSingleIterator.BiState;

import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;

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
	IInvertedIndex<NodeIDPostElement> textIndex;
	ComposeKeyBtree<NodeIDPostElement> keyIndex;
	ComposeKeyBtree<PresElement> nodePres;

	ExecutionEngine engine;

	PriorityQueue<Pair<Long, Float>> incomingQueue = new PriorityQueue<Pair<Long, Float>>();
	PriorityQueue<Pair<Long, Float>> outcomingQueue = new PriorityQueue<Pair<Long, Float>>();

	public static final float ANEAL = 0.5f;

	public Neo4jBiKeywordSearch(String path) {
		Map<String, String> config = new HashMap<String, String>();
		config.put("neostore.nodestore.db.mapped_memory", "100M");
		config.put("neostore.relationshipstore.db.mapped_memory", "200M");
		config.put("neostore.propertystore.db.mapped_memory", "200M");
		config.put("neostore.propertystore.db.strings.mapped_memory", "400M");
		config.put("neostore.propertystore.db.arrays.mapped_memory", "200M");
		config.put("keep_logical_logs", "300M size");

		graphDb = new EmbeddedGraphDatabase(path, config);
		/*	textIndex = new InvertedIndex<NodeIDPostElement>(
					new File(path, "textindex"),
					NodeIDPostElement.binding,
					(Class<Comparator<byte[]>>) NodeIDPostElement.NodeComparator.class
							.asSubclass(Comparator.class));*/
		textIndex = new CompressedInvIndex(path + "/compTextIndex");
		textIndex.init();

		keyIndex = new ComposeKeyBtree<NodeIDPostElement>(path + "/keyIndex",
				1024 * 1024 * 512, false);
		keyIndex.init(
				NodeIDPostElement.binding,
				(Class<Comparator<byte[]>>) NodeIDPostElement.NodeComparator.class
						.asSubclass(Comparator.class), true);

		nodePres = new ComposeKeyBtree<PresElement>(path + "/nodePres",
				1024 * 1024 * 512, false);
		nodePres.init(PresElement.binding,
				(Class<Comparator<byte[]>>) PresElement.NodeComparator.class
						.asSubclass(Comparator.class), true);
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
		long start = System.currentTimeMillis();
		Map<String, List<NodeIDPostElement>> results = textIndex
				.search(keywords);
		PerformanceTracker.instance.incre(PerformanceTracker.IINDEX_TIME,
				System.currentTimeMillis() - start);
		start = System.currentTimeMillis();
		List<Graph> ret = searchIntern(results, topK);
		PerformanceTracker.instance.incre(PerformanceTracker.EXPLORE_TIME,
				System.currentTimeMillis() - start);
		PerformanceTracker.instance.finishExplore();
		return ret;
	}

	@Override
	public List<Graph> search(String field, String[] keywords, int topK) {
		PerformanceTracker.instance.startExplore();
		long start = System.currentTimeMillis();
		Map<String, List<NodeIDPostElement>> results = textIndex
				.search(keywords);
		PerformanceTracker.instance.incre(PerformanceTracker.IINDEX_TIME,
				System.currentTimeMillis() - start);
		List<Graph> ret = searchIntern(results, topK);
		start = System.currentTimeMillis();
		PerformanceTracker.instance.incre(PerformanceTracker.EXPLORE_TIME,
				System.currentTimeMillis() - start);
		PerformanceTracker.instance.finishExplore();
		return ret;
	}

	private static final int maxDepth = 10;

	private List<Graph> searchIntern(
			Map<String, List<NodeIDPostElement>> results, int topK) {
		if (Constant.DEBUG_INTERM) {
			for (Entry<String, List<NodeIDPostElement>> entry : results
					.entrySet()) {
				System.out.println(String.format("key:%s;size:%d;",
						entry.getKey(), entry.getValue().size()));
			}
		}
		List<Graph> ret = null;
		HashSet<Long> seeds = new HashSet<Long>();

		for (Entry<String, List<NodeIDPostElement>> entry : results.entrySet()) {
			if (entry.getValue().size() == 0) // never return a result but
												// explore many nodes
				return new LinkedList<Graph>();
			for (NodeIDPostElement ele : entry.getValue()) {
				seeds.add(ele.getId());
			}
		}

		long start = System.currentTimeMillis();
		List<Long> seedList = new ArrayList<Long>(seeds);
		Collections.sort(seedList);
		Map<Long, Float> nodePres = getNodePres(seedList);
		PerformanceTracker.instance.incre(PerformanceTracker.PRES_INDEX_TIME,
				System.currentTimeMillis() - start);

		/*if (Constant.DEBUG_INTERM) {
			for (Entry<String, List<NodeIDPostElement>> entry : results
					.entrySet()) {
				TreeSet<Float> temp = new TreeSet<Float>();
				for (NodeIDPostElement ele : entry.getValue()) {
					temp.add(nodePres.get(ele.getId()));
				}
				System.out.println(entry.getKey());
				System.out.println(temp);
			}
		}*/

		BiState state = new BiState(results, nodePres);
		BiSingleIterator outIter = new BiSingleIterator(state);
		BiSingleIterator inIter = new BiSingleIterator(state);
		inIter.init();
		state.addPrestigeListner(outIter);
		state.addPrestigeListner(inIter);
		CandCollector collector = new CandCollector();
		state.addDistListner(collector);

		Queue<Long> roots = new LinkedList<Long>();
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
				outIter.addNode(triple);
			}

			// explore the neighbors
			if (triple.arg1 < maxDepth) {
				start = System.currentTimeMillis();
				Node node = graphDb.getNodeById(triple.arg0);
				Iterator<Relationship> iter = node.getRelationships(dir)
						.iterator();
				PerformanceTracker.instance.incre(PerformanceTracker.NEO_TIME,
						System.currentTimeMillis() - start);

				while (iter.hasNext() && roots.size() < topK) {
					start = System.currentTimeMillis();
					Relationship rel = iter.next();
					Node newNode = null;
					float dist = (Float) rel.getProperty(Constant.WEIGHT);
					PerformanceTracker.instance.incre(
							PerformanceTracker.NEO_TIME,
							System.currentTimeMillis() - start);
					if (dir == Direction.INCOMING) {
						newNode = rel.getStartNode();
						state.explore(newNode.getId(), triple.arg0, dist);
						/*	if (state.getAncestors(newNode.getId()).size() == results
									.size()) {
								System.out.println(collector.getCands().contains(
										triple.arg0));
								roots.add(newNode.getId());
							} else*/
						inIter.addNode(new Triple<Long, Integer, Float>(newNode
								.getId(), triple.arg1 + 1, state
								.getNodePres(newNode.getId())));
					} else {
						newNode = rel.getEndNode();
						state.explore(triple.arg0, newNode.getId(), dist);
						outIter.addNode(new Triple<Long, Integer, Float>(
								newNode.getId(), triple.arg1 + 1, state
										.getNodePres(newNode.getId())));
					}
				}
				/*	if (dir == Direction.OUTGOING
							&& state.getAncestors(triple.arg0).size() == results
									.size()) {
						System.out.println(collector.getCands().contains(
								triple.arg0));
						roots.add(triple.arg0);
					}*/

				// TODO need to test all the candidates here
				for (Long cand : collector.getCands()) {
					if (state.getAncestors(cand).size() == results.size()
							&& !roots.contains(cand)) {
						roots.add(cand);
					}
				}
				collector.getCands().clear();

				if (Constant.DEBUG_PROCESS) {
					System.out.println(String.format(
							"direction:%s; node: %d;page rank:%f; hits:%s",
							dir.toString(), triple.arg0,
							this.getNodePres(triple.arg0),
							state.getHits(triple.arg0).toString()));
				}
			}
			if (roots.size() >= topK)
				break;
		}
		start = System.currentTimeMillis();
		ret = constructGraphs(state, roots);
		PerformanceTracker.instance.incre(
				PerformanceTracker.CONSTRUCT_GRAPH_TIME,
				System.currentTimeMillis() - start);
		return ret;
	}

	private static class CandCollector implements NodeStateListener {
		HashSet<Long> cands = new HashSet<Long>();

		@Override
		public void notifyStateChange(long node) {
			cands.add(node);
		}

		public HashSet<Long> getCands() {
			return cands;
		}
	}

	private Map<Long, Float> getNodePres(List<Long> seeds) {
		Map<Long, Float> ret = new HashMap<Long, Float>();
		for (long seed : seeds) {
			List<PresElement> elems = nodePres.get(Long.toString(seed));
			if (elems.size() != 1) {
				System.err.println("no prestige node :" + seed);
			} else {
				ret.put(seed, elems.get(0).getId());
			}
		}
		return ret;
	}

	private Map<Long, Float> getNodePres(HashSet<Long> seeds) {
		Map<Long, Float> ret = new HashMap<Long, Float>();
		for (long seed : seeds) {
			List<PresElement> elems = nodePres.get(Long.toString(seed));
			if (elems.size() != 1) {
				System.err.println("no prestige node :" + seed);
			} else {
				ret.put(seed, elems.get(0).getId());
			}
		}
		return ret;
	}

	public float getNodePres(long node) {
		List<PresElement> elems = nodePres.get(Long.toString(node));
		if (elems.isEmpty())
			return 0.0f;
		else
			return elems.get(0).getId();
	}

	private List<Graph> constructGraphs(BiState state, Queue<Long> result) {
		List<Graph> graphes = new LinkedList<Graph>();
		for (long root : result) {
			HashMap<String, Long> sources = state.getAncestors(root);
			Graph graph = new Graph();
			for (Entry<String, Long> source : sources.entrySet()) {
				long temp = root;
				long pre = source.getValue();
				do {
					if (pre != temp) // the case root == pre
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
		if (opCount++ % 5000 == 0) {
			textIndex.flush();
			keyIndex.flush();
		}
	}

	/**
	 * load the graph into memory and calculate it
	 */
	public void calcNodePres() {
		long begin = System.currentTimeMillis();
		// TODO implements the pagerank algorithm here
		// load the map into memory
		HashMap<Long, Integer> outCount = new HashMap<Long, Integer>();
		// a <- b
		HashMap<Long, HashSet<Long>> matrix = new HashMap<Long, HashSet<Long>>();
		for (Node node : graphDb.getAllNodes()) {
			HashSet<Long> vector = new HashSet<Long>();
			matrix.put(node.getId(), vector);
			for (Relationship rel : node.getRelationships(Direction.INCOMING)) {
				long start = rel.getStartNode().getId();
				vector.add(start);
				if (!outCount.containsKey(start)) {
					outCount.put(start, 1);
				} else {
					outCount.put(start, outCount.get(start) + 1);
				}
			}
		}

		System.out.println("load graph takes "
				+ (System.currentTimeMillis() - begin) / 1000.0f + "s");
		begin = System.currentTimeMillis();
		// calculate the page rank
		HashMap<Long, Float> newNodePresMap = new HashMap<Long, Float>();
		HashMap<Long, Float> curNodePresMap = new HashMap<Long, Float>();

		int loop = 20;
		float rProb = 0.1f;// with rProb probability choose another node
		for (int i = 0; i < loop; i++) {
			for (long endPoint : matrix.keySet()) {
				float newPres = 0.0f;
				for (long startPoint : matrix.get(endPoint)) {
					float nodePres = 1.0f;
					if (curNodePresMap.containsKey(startPoint)) {
						nodePres = curNodePresMap.get(startPoint);
					}
					newPres += nodePres / outCount.get(startPoint);
				}
				newPres = rProb / matrix.size() + (1.0f - rProb) * newPres;
				newNodePresMap.put(endPoint, newPres);
			}
			curNodePresMap.clear();
			curNodePresMap = newNodePresMap;
			newNodePresMap = new HashMap<Long, Float>();
			System.out.println("loop " + i + "  takes "
					+ (System.currentTimeMillis() - begin) / 1000.0f + "s");
			begin = System.currentTimeMillis();
		}

		for (Entry<Long, Float> rank : curNodePresMap.entrySet()) {
			nodePres.put(Long.toString(rank.getKey()),
					new PresElement(rank.getValue()));
		}
		System.out.println("flush to disk takes "
				+ (System.currentTimeMillis() - begin) / 1000.0f + "s");
		nodePres.flush();
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
		nodePres.close();
	}

	public void flush() {
		keyIndex.flush();
		textIndex.flush();
		nodePres.flush();
	}

	public void printPerform() {
		System.out.println(String.format(
				"btree: recent put:%f;\nrecent retrieve:%f\n",
				keyIndex.getRecentPutLatency(),
				keyIndex.getRecentRetrievelLatency()));
		// textIndex.printPerformance();
	}

	private static final String dblpPath = "data/dblp.txt";
	public static final String graphPath = "data/bidata";

	public static void main(String[] args) throws IOException {
		// FileUtil.delete(graphPath);
		/*
		 DBLPPaper paper = null;
		DblpFileReader reader = new DblpFileReader(dblpPath);
		Neo4jBiKeywordSearch graph = new Neo4jBiKeywordSearch(graphPath);

		List<Pair<Pair<String, Boolean>, Pair<String, Boolean>>> edges = null;

		long start = System.currentTimeMillis();
		int count = 0;
		long total = 0;
		while (null != (paper = reader.nextPaper())) {
			edges = new LinkedList<Pair<Pair<String, Boolean>, Pair<String, Boolean>>>();
			// id -> venue
			edges.add(new Pair<Pair<String, Boolean>, Pair<String, Boolean>>(
					new Pair<String, Boolean>(paper.id, true),
					new Pair<String, Boolean>(paper.venue, true)));
			// id -> abstracts
			if (!paper.abstracts.isEmpty())
				edges.add(new Pair<Pair<String, Boolean>, Pair<String, Boolean>>(
						new Pair<String, Boolean>(paper.id, true),
						new Pair<String, Boolean>(paper.abstracts, false)));
			// id->year
			edges.add(new Pair<Pair<String, Boolean>, Pair<String, Boolean>>(
					new Pair<String, Boolean>(paper.id, true),
					new Pair<String, Boolean>(paper.year, true)));

			// id->year
			edges.add(new Pair<Pair<String, Boolean>, Pair<String, Boolean>>(
					new Pair<String, Boolean>(paper.id, true),
					new Pair<String, Boolean>(paper.title, true)));

			// id <- refIDs
			for (String refID : paper.refIDs) {
				edges.add(new Pair<Pair<String, Boolean>, Pair<String, Boolean>>(
						new Pair<String, Boolean>(refID, true),
						new Pair<String, Boolean>(paper.id, true)));
			}
			// id-> authors
			for (String author : paper.authors) {
				edges.add(new Pair<Pair<String, Boolean>, Pair<String, Boolean>>(
						new Pair<String, Boolean>(paper.id, true),
						new Pair<String, Boolean>(author, true)));
				edges.add(new Pair<Pair<String, Boolean>, Pair<String, Boolean>>(
						new Pair<String, Boolean>(author, true),
						new Pair<String, Boolean>(paper.id, true)));
			}
			graph.addSubgraph(edges, DBLPTest.construct(edges.size()));

			if (count++ % 100 == 0) {
				long cost = (System.currentTimeMillis() - start);
				total += cost;
				System.out.println(String.format(
						"opcount: %d;total time:%f s; time for last 100:%f s",
						count, total / 1000.0f, cost / 1000.0f));
				start = System.currentTimeMillis();
				graph.printPerform();
			}
			// if (count > 11000)
			// break;
		}
		System.out.println("load graph complete");
		graph.close();
		*/
		Neo4jBiKeywordSearch graph = null;
		try {
			graph = new Neo4jBiKeywordSearch(graphPath);
			graph.calcNodePres();
		} finally {
			if (graph != null)
				graph.close();
		}
	}

	@Override
	public String toString() {
		return "Neo4jBiKeywordSearch []";
	}
}

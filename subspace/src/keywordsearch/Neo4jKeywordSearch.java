package keywordsearch;

import graph.Graph;
import graph.KeywordSearch;
import graph.MyRelationshipTypes;

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

import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexHits;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import util.Constant;
import util.DBLPPaper;
import util.DblpFileReader;
import xiafan.file.FileUtil;
import xiafan.util.Pair;

/**
 * 测试一下ShortestPathIterator，两个同时遍历的时候有问题
 * 
 * @author xiafan
 * 
 */
public class Neo4jKeywordSearch implements KeywordSearch {
	GraphDatabaseService graphDb;
	// Index<Node> nodeIndex;
	InvertedIndex<NodeIDPostElement> textIndex;

	Index<Node> keyIndex;
	ExecutionEngine engine;

	public Neo4jKeywordSearch(String path) {
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
		/*
		nodeIndex = graphDb.index().forNodes(
				"message",
				MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type",
						"fulltext", "to_low_case", "true", "analyzer",
						"org.apache.lucene.analysis.SimpleAnalyzer"));

		keyIndex = graphDb.index().forNodes(
				"key",
				MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type",
						"fulltext", "to_low_case", "true"));
		// engine = new ExecutionEngine(graphDb, null);
		 */

	}

	/*
		public void test() {
			Transaction tnx = graphDb.beginTx();
			Node firstNode = graphDb.createNode();// 创建第一个节点
			Node secondNode = graphDb.createNode();// 创建第二个节点
			// 节点firstNode通过createRelationshipTo函数跟节点secondNode建立KNOWS联系
			Relationship relationship = firstNode.createRelationshipTo(secondNode,
					MyRelationshipTypes.KNOWS);

			firstNode.setProperty("message", "Hello,world, this is a fucking test");// 设置“键-值”这样类型的属性
			secondNode.setProperty("message", "world!");
			relationship.setProperty("message", "brave Neo4j ");
			nodeIndex.add(firstNode, "message", firstNode.getProperty("message"));

			tnx.success();
			tnx.finish();
			// engine.execute("create (usera{message, 'hello world'})");
			System.out.println(graphDb.getNodeById(1).getProperty("message"));
			IndexHits<Node> hits = nodeIndex.query("message", "Hello");
			System.out.println("size:" + hits.size());
			while (hits.hasNext()) {
				System.out.println(hits.next());
			}

			hits = nodeIndex.query("message", "Hello,world");
			System.out.println("size:" + hits.size());
			while (hits.hasNext()) {
				System.out.println(hits.next());
			}

			hits = nodeIndex.query("message", "fucking test");
			System.out.println("size:" + hits.size());
			while (hits.hasNext()) {
				System.out.println(hits.next());
			}
			graphDb.shutdown();
		}
	*/
	/**
	 * map : a->b pair : startNode, whether index the key
	 * 
	 * @param subgraph
	 */
	@Override
	public void addSubgraph(
			List<Pair<Pair<String, Boolean>, Pair<String, Boolean>>> edges,
			List<Float> weights) {
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
				// if (entry.arg0.arg1)
				// keyIndex.add(startNode, Constant.KEY, entry.arg0.arg0);
			}

			endNode = this.getNode(entry.arg1.arg0);
			if (endNode == null) {
				endNode = graphDb.createNode();
				endNode.setProperty(Constant.KEY, entry.arg1.arg0);
				// nodeIndex.add(endNode, Constant.KEY, entry.arg1.arg0);
				// if (entry.arg1.arg1)
				// keyIndex.add(endNode, Constant.KEY, entry.arg1.arg0);
			}

			float weight = iter.next();
			Relationship ship = startNode.createRelationshipTo(endNode,
					MyRelationshipTypes.KNOWS);
			ship.setProperty(Constant.WEIGHT, weight);
		}
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
				keyIndex.add(node, Constant.KEY, key);
			tnx.success();
		} catch (IOException e) {
			tnx.failure();
			node = null;
		} finally {
			tnx.finish();
		}
		return node;
	}

	public Node addNode(String post) {
		Transaction tnx = graphDb.beginTx();
		Node node = graphDb.createNode();
		node.setProperty("post", post);
		// nodeIndex.add(node, "post", node.getProperty("post"));
		keyIndex.add(node, Constant.KEY, post);
		tnx.success();
		tnx.finish();

		return node;
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

	/**
	 * return topK graph
	 */
	@Override
	public List<Graph> search(String field, String[] keywords) {
		return search(field, keywords, 10);
	}

	@Override
	public List<Graph> search(String field, String[] keywords, int topK) {
		DjskState state = new DjskState();
		// HashMap<String, IndexHits<Node>> postMap = new HashMap<String,
		// IndexHits<Node>>();
		List<ShortestNodeIterator> iterList = new LinkedList<ShortestNodeIterator>();
		HitBroad broad = new HitBroad(keywords.length);

		// 初始化hitbroad和每个node对应的iterator
		for (String keyword : keywords) {
			/*IndexHits<Node> hits = nodeIndex.query(field, keyword);
			while (hits.hasNext()) {
				Node nextNode = hits.next();
				broad.addKeyword(nextNode.getId(), keyword);
				iterList.add(new ShortestNodeIterator(nextNode.getId(),
						graphDb, state, MyRelationshipTypes.KNOWS, 8));
			}
			postMap.put(keyword, hits);
			*/
			Map<String, List<NodeIDPostElement>> results = textIndex
					.search(keywords);
			for (Entry<String, List<NodeIDPostElement>> entry : results
					.entrySet()) {
				for (NodeIDPostElement element : entry.getValue()) {
					broad.addKeyword(element.getId(), entry.getKey());
					iterList.add(new ShortestNodeIterator(element.getId(),
							graphDb, state, null, 8));
				}
			}
		}

		// HashMap<Long, HashSet<Long>> nodeHits = new HashMap<Long,
		// HashSet<Long>>();
		HashSet<Long> result = new HashSet<Long>();

		// 探索每个keyword，按照距离最短的方式进行探索
		ShortestNodeIterator smallestIter = null;
		int retNum = 0;
		do {
			smallestIter = shortestIter(iterList);
			if (smallestIter != null) {
				Pair<Long, Float> item = smallestIter.next();
				broad.hit(item.arg0, smallestIter.getSource());
				if (broad.validate(item.arg0)) {
					// 当前探索的这个节点已经能够到达所有的keyword，可以返回
					result.add(item.arg0);
					retNum++;
				}
			}
		} while (smallestIter != null && retNum < topK);
		return constructGraphs(state, broad, result);
	}

	private List<Graph> constructGraphs(DjskState state, HitBroad broad,
			HashSet<Long> result) {
		List<Graph> graphes = new LinkedList<Graph>();

		for (long root : result) {
			HashSet<Long> sources = broad.getHits(root);
			Graph graph = new Graph();
			for (long source : sources) {
				long temp = root;
				long pre = -1;
				do {
					pre = state.preNode(source, temp);
					graph.addEdge(pre, temp);
					temp = pre;
				} while (pre != source);
			}
			graphes.add(graph);
		}
		return graphes;
	}

	private static class HitBroad {
		HashMap<Long, HashSet<Long>> nodeHits = new HashMap<Long, HashSet<Long>>();
		HashMap<Long, HashSet<String>> keywordHits = new HashMap<Long, HashSet<String>>();
		HashMap<Long, HashSet<String>> postMap = new HashMap<Long, HashSet<String>>();
		int keywordNum;

		public HitBroad(int keywordNum) {
			this.keywordNum = keywordNum;
		}

		public void addKeyword(long node, String keyword) {
			if (!postMap.containsKey(node)) {
				postMap.put(node, new HashSet<String>());
			}
			postMap.get(node).add(keyword);
		}

		public void hit(long root, long source) {
			for (String keyword : postMap.get(source)) {
				if (!nodeHits.containsKey(root)) {
					nodeHits.put(root, new HashSet<Long>());
				}
				nodeHits.get(root).add(source);

				if (!keywordHits.containsKey(root)) {
					keywordHits.put(root, new HashSet<String>());
				}
				keywordHits.get(root).add(keyword);
			}
		}

		public HashSet<Long> getHits(long node) {
			return nodeHits.get(node);
		}

		public boolean validate(long node) {
			if (keywordHits.get(node).size() == keywordNum)
				return true;
			return false;
		}
	}

	private static ShortestNodeIterator shortestIter(
			List<ShortestNodeIterator> iterList) {
		ShortestNodeIterator smallestIter = null;
		for (Iterator<ShortestNodeIterator> iter = iterList.iterator(); iter
				.hasNext();) {
			ShortestNodeIterator sIter = iter.next();
			if (sIter.hasNext()) {
				if (smallestIter != null) {
					if (sIter.poll().arg1 < smallestIter.poll().arg1) {
						smallestIter = sIter;
					}
				} else {
					smallestIter = sIter;
				}
			} else {
				iter.remove();
			}
		}
		return smallestIter;
	}

	public static class DjskState {
		HashMap<Pair<Long, Long>, Float> distance = new HashMap<Pair<Long, Long>, Float>();
		HashMap<Pair<Long, Long>, Long> path = new HashMap<Pair<Long, Long>, Long>();

		public float distance(long start, long end) {
			Pair<Long, Long> key = new Pair<Long, Long>(start, end);
			if (!distance.containsKey(key)) {
				return 100000000.0f;
			} else {
				return distance.get(key);
			}
		}

		public long preNode(long start, long end) {
			return path.get(new Pair<Long, Long>(start, end));
		}

		public void addDistance(long source, long cur, long pre, float dist) {
			distance.put(new Pair<Long, Long>(source, cur), dist);
			path.put(new Pair<Long, Long>(source, cur), pre);
		}
	}

	public static void main(String[] args) throws IOException {
		FileUtil.delete("var/test");
		Neo4jKeywordSearch search = new Neo4jKeywordSearch("var/test");
		Node p = search.addNode("tree calculated");
		Node c1 = search.addNode("Prim algorithm");
		Node c2 = search.addNode("includes destination");

		search.addEdge(c1, p, 1.0f);
		search.addEdge(c2, p, 1.0f);

		Node c3 = search.addNode("which means");
		Node c4 = search.addNode("Steiner tree");
		Node c5 = search.addNode("spanning tree");
		search.addEdge(c3, c2, 1.0f);
		search.addEdge(c4, c2, 1.0f);
		search.addEdge(c5, c1, 1.0f);

		List<Graph> graphes = search.search("post", new String[] { "steiner",
				"means", "spanning" });
		System.out.println(graphes);

		System.out.println(search.getNode("which means"));

		// search.test();
	}

	@Override
	public Node getNode(String key) {
		IndexHits<Node> hits = keyIndex.get(Constant.KEY, key);
		if (hits.hasNext())
			return hits.next();
		else
			return null;
	}

	public List<Node> getNodes(String key) {
		IndexHits<Node> hits = keyIndex.get(Constant.KEY, key);
		List<Node> ret = new LinkedList<Node>();
		while (hits.hasNext()) {
			ret.add(hits.next());
		}
		return ret;
	}

	@Override
	public void close() {
		graphDb.shutdown();
	}
}

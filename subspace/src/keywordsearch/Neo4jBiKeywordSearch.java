package keywordsearch;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import keywordsearch.Neo4jKeywordSearch.DjskState;

import org.neo4j.cypher.ExecutionEngine;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.index.Index;
import org.neo4j.graphdb.index.IndexManager;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.EmbeddedGraphDatabase;

import util.Constant;
import xiafan.util.Pair;

import graph.Graph;
import graph.KeywordSearch;
import graph.MyRelationshipTypes;

public class Neo4jBiKeywordSearch implements KeywordSearch {
	GraphDatabaseService graphDb;
	Index<Node> nodeIndex;
	ExecutionEngine engine;

	PriorityQueue<Pair<Long, Float>> incomingQueue = new PriorityQueue<Pair<Long, Float>>();
	PriorityQueue<Pair<Long, Float>> outcomingQueue = new PriorityQueue<Pair<Long, Float>>();

	public static final float ANEAL = 0.5f;

	public Neo4jBiKeywordSearch(String path) {
		graphDb = new EmbeddedGraphDatabase(path);
		nodeIndex = graphDb.index().forNodes(
				"message",
				MapUtil.stringMap(IndexManager.PROVIDER, "lucene", "type",
						"fulltext", "to_low_case", "true", "analyzer",
						"org.apache.lucene.analysis.SimpleAnalyzer"));
	}

	@Override
	public Node addNode(String key, Map<String, String> property,
			String indexField) {
		Transaction tnx = graphDb.beginTx();
		Node node = graphDb.createNode();
		node.setProperty(Constant.KEY, key);
		for (Entry<String, String> entry : property.entrySet()) {
			node.setProperty(entry.getKey(), entry.getValue());
		}
		nodeIndex.add(node, indexField, node.getProperty(indexField));
		nodeIndex.add(node, Constant.KEY, key);
		tnx.success();
		tnx.finish();
		return node;
	}

	@Override
	public void addEdge(Node start, Node end) {
		Transaction tnx = graphDb.beginTx();
		start.createRelationshipTo(end, MyRelationshipTypes.KNOWS);
		tnx.success();
		tnx.finish();
	}

	@Override
	public Node getNode(String key) {
		return (Node) nodeIndex.get(Constant.KEY, key);
	}

	@Override
	public List<Graph> search(String field, String[] keywords) {
		return search(field, keywords, 10);
	}

	@Override
	public List<Graph> search(String field, String[] keywords, int topK) {
		// TODO Auto-generated method stub

		return null;
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

}

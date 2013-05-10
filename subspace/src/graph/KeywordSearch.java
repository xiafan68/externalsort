package graph;

import java.util.List;
import java.util.Map;
import org.neo4j.graphdb.Node;

import xiafan.util.Pair;

/**
 * neo4j based keyword search
 * 
 * @author xiafan
 * 
 */
public interface KeywordSearch {

	public Node getNode(String key);

	public List<Graph> search(String field, String[] keywords);

	public List<Graph> search(String field, String[] keywords, int topK);

	void addEdge(Node start, Node end, float weight);

	Node addNode(String key, boolean iindexKey, Map<String, String> property,
			String indexField);

	public void close();

	void addSubgraph(
			List<Pair<Pair<String, Boolean>, Pair<String, Boolean>>> edges,
			List<Float> weights);
}

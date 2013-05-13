package keywordsearch;

import keywordsearch.backward.Neo4jKeywordSearch;
import keywordsearch.bidirection.Neo4jBiKeywordSearch;
import graph.KeywordSearch;

public class KWSearchFactory {
	public static KWSearchFactory instance = new KWSearchFactory();

	public KeywordSearch get(String name, String path) {
		if (name.equals("bidirection")) {
			return new Neo4jBiKeywordSearch(path);
		} else {
			return new Neo4jKeywordSearch(path);
		}
	}
}

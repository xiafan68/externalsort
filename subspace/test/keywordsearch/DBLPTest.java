package keywordsearch;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import org.junit.Test;

import util.DBLPPaper;
import util.DblpFileReader;
import xiafan.util.Pair;

public class DBLPTest {
	private static final String dblpPath = "data/dblp.txt";
	private static final String graphPath = "data/graphdata";
	private static TreeMap<Integer, List<Float>> weightMap = new TreeMap<Integer, List<Float>>();

	private static List<Float> weights = new LinkedList<Float>();

	private static List<Float> construct(int size) {
		/*
		Integer listIndex = weightMap.ceilingKey(size);
		if (listIndex == null) {
			List<Float> list = 
		} else
			return weightMap.get(listIndex);
		*/
		if (size > weights.size()) {
			for (int i = 0; i < size - weights.size(); i++)
				weights.add(1.0f);
		}
		return weights;
	}

	@Test
	public void test() throws IOException {
		DBLPPaper paper = null;
		DblpFileReader reader = new DblpFileReader(dblpPath);
		Neo4jKeywordSearch graph = new Neo4jKeywordSearch(graphPath);
		List<Pair<Pair<String, Boolean>, Pair<String, Boolean>>> edges = null;

		long start = System.currentTimeMillis();
		int count = 0;
		while (null != (paper = reader.nextPaper())) {
			edges = new LinkedList<Pair<Pair<String, Boolean>, Pair<String, Boolean>>>();
			// id -> venue
			edges.add(new Pair<Pair<String, Boolean>, Pair<String, Boolean>>(
					new Pair<String, Boolean>(paper.venue, true),
					new Pair<String, Boolean>(paper.id, true)));
			// id -> abstracts
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
			}
			graph.addSubgraph(edges, construct(edges.size()));

			if (count++ % 100 == 0) {
				System.out.println(String.format(
						"opcount: %d;time for last 100:%f s", count,
						System.currentTimeMillis() - start));
				start = System.currentTimeMillis();
			}
		}
		graph.close();
	}
}

package keywordsearch;

import graph.Graph;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import keywordsearch.backward.Neo4jKeywordSearch;

import org.junit.Test;
import org.neo4j.graphdb.Node;

import util.Constant;
import util.DBLPPaper;
import util.DblpFileReader;
import xiafan.file.FileUtil;
import xiafan.util.Pair;

public class DBLPTest {
	private static final String dblpPath = "data/dblp.txt";
	private static final String graphPath = "data/graphdata";
	private static TreeMap<Integer, List<Float>> weightMap = new TreeMap<Integer, List<Float>>();

	private static List<Float> weights = new LinkedList<Float>();

	public static List<Float> construct(int size) {
		/*
		Integer listIndex = weightMap.ceilingKey(size);
		if (listIndex == null) {
			List<Float> list = 
		} else
			return weightMap.get(listIndex);
		*/
		if (size > weights.size()) {
			int gap = size - weights.size();
			for (int i = 0; i < gap; i++)
				weights.add(1.0f);
		}
		return weights;
	}

	@Test
	public void test() throws IOException {
		FileUtil.delete(graphPath);
		DBLPPaper paper = null;
		DblpFileReader reader = new DblpFileReader(dblpPath);
		Neo4jKeywordSearch graph = new Neo4jKeywordSearch(graphPath);

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
			graph.addSubgraph(edges, construct(edges.size()));

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
		graph.close();
	}

	@Test
	public void query() throws IOException {
		Neo4jKeywordSearch graphDb = new Neo4jKeywordSearch(graphPath);
		System.out.println(graphDb.search("", "keyword search", 4));
		for (Graph graph : graphDb.search("", "keywords search", 4)) {
			printGraph(graphDb, graph);
		}
		graphDb.close();
	}

	private static void printGraph(Neo4jKeywordSearch graphDb, Graph graph) {
		Iterator<Pair<Long, Long>> iter = graph.edgeIter();
		while (iter.hasNext()) {
			Pair<Long, Long> edge = iter.next();
			Node start = graphDb.getNodeById(edge.arg0);
			Node end = graphDb.getNodeById(edge.arg1);
			System.out.println(start.getProperty(Constant.KEY) + "->"
					+ end.getProperty(Constant.KEY));
		}
	}

	@Test
	public void correctnessTest() throws IOException {
		FileUtil.delete(graphPath);
		Neo4jKeywordSearch graph = new Neo4jKeywordSearch(graphPath);

		List<Pair<Pair<String, Boolean>, Pair<String, Boolean>>> edges = new LinkedList<Pair<Pair<String, Boolean>, Pair<String, Boolean>>>();

		edges.add(new Pair<Pair<String, Boolean>, Pair<String, Boolean>>(
				new Pair<String, Boolean>("matrix revolution", true),
				new Pair<String, Boolean>("neo the one", true)));
		edges.add(new Pair<Pair<String, Boolean>, Pair<String, Boolean>>(
				new Pair<String, Boolean>("matrix revolution", true),
				new Pair<String, Boolean>("trinity the lover", true)));
		graph.addSubgraph(edges, construct(edges.size()));
		System.out.println(graph.getNodes("matrix revolution"));
		System.out.println(graph.getNode("trinity the lover"));
		System.out.println(graph.search("", "neo the one", 2));

	}
}

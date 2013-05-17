package keywordsearch;

import graph.Graph;
import graph.KeywordSearch;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

import org.neo4j.graphdb.Node;

import util.Constant;
import util.PerformanceTracker;
import xiafan.file.FileUtil;
import xiafan.util.Pair;

public class KWSearchClient {
	// private static final String graphPath = "data/graphdata";

	private static void usage() {
		System.out
				.println("usage:\n"
						+ "q: query words topK  --- to launch keyword search and return integer words\n"
						+ "qd: sentence ---- retrieve related nodes\n"
						+ "quit --- quit\n"
						+ "save path --- save the graph results in the directory of path\n"
						+ "del path --- delete the directory of path");
		System.out.print(">");
	}

	public static void main(String[] args) throws IOException {
		// KeywordSearch graphDb = new Neo4jKeywordSearch(graphPath);
		Properties prop = new Properties();
		prop.load(new FileInputStream("config.property"));
		KeywordSearch graphDb = KWSearchFactory.instance.get(
				prop.getProperty("class"), prop.getProperty("path"));
		System.out.println(prop);
		// KeywordSearch graphDb = new Neo4jBiKeywordSearch("data/bidata");
		Scanner scanner = new Scanner(System.in);
		String line = null;
		usage();
		try {
			List<Graph> curResult = null;
			while (null != (line = scanner.nextLine())) {
				long start = System.currentTimeMillis();
				long cost = 0;
				if (line.startsWith("q:")) {
					// this is q keyword query
					String query = line.substring("q:".length(),
							line.lastIndexOf(" ")).trim();
					Integer topK = null;
					try {
						topK = Integer.parseInt(line.substring(
								line.lastIndexOf(" ")).trim());
					} catch (Exception ex) {
						usage();
						continue;
					}
					curResult = graphDb.search("", query, topK);
					cost = System.currentTimeMillis() - start;
					System.out.println("num of results:" + curResult.size());
					for (Graph graph : curResult) {
						printGraph(graphDb, graph);
					}
				} else if (line.startsWith("qd:")) {
					List<Node> nodes = graphDb.getNodes(line.substring(
							"qd:".length()).trim());
					System.out.println("results size:" + nodes.size());
					for (Node node : nodes) {
						System.out.println(node.getProperty(Constant.KEY));
					}
				} else if (line.startsWith("quit")) {
					break;
				} else if (line.startsWith("save")) {
					String path = line.substring("save".length()).trim();
					FileUtil.delete(path);
					int count = 0;
					for (Graph graph : curResult) {
						saveGraph(graphDb, graph, new File(path, (count++)
								+ ".csv").getAbsolutePath());
					}
				} else if (line.startsWith("del:")) {
					FileUtil.delete(line.substring("del:".length()).trim());
				}
				cost = (cost == 0 ? (System.currentTimeMillis() - start) : cost);
				System.out.println(String.format("time elapsed:%fs",
						cost / 1000.0f));
				PerformanceTracker.instance.print();
				System.out.print(">");
			}
		} finally {
			PerformanceTracker.instance.print();
			graphDb.close();
		}
	}

	private static void saveGraph(KeywordSearch graphDb, Graph graph,
			String path) throws UnsupportedEncodingException, IOException {
		File file = new File(path);
		if (!file.exists()) {
			if (file.getParentFile() != null)
				file.getParentFile().mkdirs();
			file.createNewFile();
		}

		DataOutputStream output = new DataOutputStream(new FileOutputStream(
				path, true));
		/*Iterator<Long> nodeIter = graph.nodeIter();
		while (nodeIter.hasNext()) {
			Long sID = nodeIter.next();
			Node start = graphDb.getNodeById(sID);
			output.write(String
					.format("%d\t%s\n", sID, start.getProperty(Constant.KEY))
					.replace(' ', '_').getBytes("UTF-8"));
			Iterator<Long> iter = graph.getTargetNode(sID);
			while (iter.hasNext()) {
				// sid;tid
				long tID = iter.next();
				output.write(String.format("%d\t%d\n", sID, tID).getBytes(
						"UTF-8"));

				Node target = graphDb.getNodeById(tID);
				output.write(String
						.format("%d\t%s\n", tID,
								target.getProperty(Constant.KEY))
						.replace(' ', '_').getBytes("UTF-8"));

				// output.write("\n".getBytes("UTF-8"));
			}

			
		}*/

		Iterator<Pair<Long, Long>> iter = graph.edgeIter();
		// System.out.println("new graph:");
		while (iter.hasNext()) {
			Pair<Long, Long> edge = iter.next();
			Node start = graphDb.getNodeById(edge.arg0);
			Node end = graphDb.getNodeById(edge.arg1);
			output.write(String.format(
					"%s\t%s\n",
					((String) start.getProperty(Constant.KEY)).replaceAll(" ",
							"_"),
					((String) end.getProperty(Constant.KEY)).replaceAll(" ",
							"_")).getBytes("UTF-8"));
		}
		output.close();
	}

	private static void printGraph(KeywordSearch graphDb, Graph graph) {
		Iterator<Pair<Long, Long>> iter = graph.edgeIter();
		System.out.println("new graph:");
		while (iter.hasNext()) {
			Pair<Long, Long> edge = iter.next();
			Node start = graphDb.getNodeById(edge.arg0);
			Node end = graphDb.getNodeById(edge.arg1);
			System.out.println(start.getProperty(Constant.KEY) + "->"
					+ end.getProperty(Constant.KEY));
		}
	}
}

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Created with IntelliJ IDEA. User: xiafan Date: 5/3/13 Time: 4:21 PM To change
 * this template use File | Settings | File Templates.
 */
public class AdjMatrixTransform {
	public static HashMap<String, Integer> unameToIDMapping = new HashMap<String, Integer>();
	public static TreeMap<Integer, TreeSet<Integer>> adjMatrix = new TreeMap<Integer, TreeSet<Integer>>();
	public static int idGen = 1;

	public static void transform(String[] args) throws IOException {
		BufferedReader dis = new BufferedReader(new InputStreamReader(
				new FileInputStream(args[0]), "gbk"));
		DataOutputStream uToIDFile = new DataOutputStream(new FileOutputStream(
				args[1]));
		DataOutputStream adjMatrixFile = new DataOutputStream(
				new FileOutputStream(args[2]));

		String line = null;
		int edgeCount = 0;
		while (null != (line = dis.readLine())) {
			if (edgeCount++ == 0)
				continue;
			String[] uids = line.split("\t");
			if (!unameToIDMapping.containsKey(uids[0])) {
				unameToIDMapping.put(uids[0], idGen++);
			}
			if (!unameToIDMapping.containsKey(uids[1])) {
				unameToIDMapping.put(uids[1], idGen++);
			}

			if (!adjMatrix.containsKey(unameToIDMapping.get(uids[0]))) {
				adjMatrix.put(unameToIDMapping.get(uids[0]),
						new TreeSet<Integer>());
			}

			if (!adjMatrix.containsKey(unameToIDMapping.get(uids[1]))) {
				adjMatrix.put(unameToIDMapping.get(uids[1]),
						new TreeSet<Integer>());
			}

			adjMatrix.get(unameToIDMapping.get(uids[0])).add(
					unameToIDMapping.get(uids[1]));
			adjMatrix.get(unameToIDMapping.get(uids[1])).add(
					unameToIDMapping.get(uids[0]));
		}

		for (Entry<String, Integer> entry : unameToIDMapping.entrySet()) {
			uToIDFile.write(String.format("%s\t%d\n", entry.getKey(),
					entry.getValue()).getBytes());
		}

		// flush matrix
		edgeCount = 0;
		for (Entry<Integer, TreeSet<Integer>> entry : adjMatrix.entrySet()) {
			StringBuffer buffer = new StringBuffer();
			// buffer.append(entry.getKey());
			for (Integer flw : entry.getValue()) {
				edgeCount++;
				buffer.append("\t");
				buffer.append(flw);
			}

			buffer.append("\n");
			System.out.println(buffer.substring(1));
			adjMatrixFile.write(buffer.substring(1).getBytes());
		}

		uToIDFile.close();
		adjMatrixFile.close();
		System.out.println("edge count:" + edgeCount);
		System.out.println("node count:" + unameToIDMapping.size());
	}

	public static void replay(String[] args) throws NumberFormatException,
			IOException {
		BufferedReader mappingReader = new BufferedReader(
				new InputStreamReader(new FileInputStream(args[0])));
		BufferedReader clusterReader = new BufferedReader(
				new InputStreamReader(new FileInputStream(args[1])));
		DataOutputStream clusterMappingFile = new DataOutputStream(
				new FileOutputStream(args[2]));
		HashMap<Integer, String> idToUNameMapping = new HashMap<Integer, String>();
		String line = null;
		while (null != (line = mappingReader.readLine())) {
			String[] mapping = line.split("\t");
			idToUNameMapping.put(Integer.parseInt(mapping[1]), mapping[0]);
		}

		int id = 1;
		TreeMap<Integer, LinkedList<String>> outMap = new TreeMap<Integer, LinkedList<String>>();
		outMap.put(0, new LinkedList<String>());
		outMap.put(1, new LinkedList<String>());
		outMap.put(2, new LinkedList<String>());
		while (null != (line = clusterReader.readLine())) {
			outMap.get(Integer.parseInt(line.trim())).add(
					idToUNameMapping.get(id++));
		}
		for (Entry<Integer, LinkedList<String>> entry : outMap.entrySet()) {
			for (String user : entry.getValue())
				clusterMappingFile.write(String.format("%d\t%s\n",
						entry.getKey(), user).getBytes());
		}

		clusterMappingFile.close();

	}

	public static void main(String[] args) throws IOException {
		replay(new String[] { "uname.txt", "cluster.txt", "clsmapping.txt" });
	}
}

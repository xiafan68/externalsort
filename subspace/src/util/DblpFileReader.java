package util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class DblpFileReader {
	BufferedReader bReader;

	public DblpFileReader(String file) throws FileNotFoundException {
		FileReader reader = new FileReader(file);
		bReader = new BufferedReader(reader);

	}

	/*
	 * #* --- paperTitle #@ --- Authors #t ---- Year #c --- publication venue
	 * #index 00---- index id of this paper #% ---- the id of references of this
	 * paper (there are multiple lines, with each indicating a reference) #! ---
	 * Abstract
	 */
	public DBLPPaper nextPaper() throws IOException {
		DBLPPaper ret = null;
		String line = null;
		while (null != (line = bReader.readLine())) {
			if (line.startsWith("#*")) {
				ret = new DBLPPaper();
				ret.title = line.substring(2);
			} else {
				if (ret == null)
					continue;
				if (line.startsWith("#@")) {
					for (String author : line.substring(2).split(","))
						ret.addAuthor(author);
				} else if (line.startsWith("#t")) {
					ret.year = line.substring(2);
				} else if (line.startsWith("#c")) {
					ret.venue = line.substring(2);
				} else if (line.startsWith("#index")) {
					ret.id = line.substring("#index".length());
				} else if (line.startsWith("#%")) {
					ret.addRef(line.substring("#%".length()));
				} else if (line.startsWith("#!")) {
					ret.abstracts = line.substring(2);
					break;
				}
			}
		}

		return ret;
	}

	public static void main(String[] args) throws IOException {
		/*
		 * #* --- paperTitle #@ --- Authors #t ---- Year #c --- publication
		 * venue #index 00---- index id of this paper #% ---- the id of
		 * references of this paper (there are multiple lines, with each
		 * indicating a reference) #! --- Abstract
		 */
		DblpFileReader reader = new DblpFileReader("data/dblp.txt");
		DBLPPaper paper = null;
		int count = 0;
		while (null != (paper = reader.nextPaper())) {

			// Node paperNode = search.addNode(paper.id, paper.toMap(), "ab");
			if (!paper.refIDs.isEmpty() && !paper.venue.isEmpty()) {
				System.out.println(paper);
				if (count++ > 10)
					break;
			}

		}
		reader.close();
	}

	private void close() throws IOException {
		bReader.close();
	}
}

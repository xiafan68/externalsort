package util;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.neo4j.helpers.collection.MapUtil;

/*
 * #* --- paperTitle #@ --- Authors #t ---- Year #c --- publication
 * venue #index 00---- index id of this paper #% ---- the id of
 * references of this paper (there are multiple lines, with each
 * indicating a reference) #! --- Abstract
 */
public class DBLPPaper {
	public String id;
	public String title;
	public List<String> authors = new LinkedList<String>();
	public String year;
	public String venue;
	public String abstracts;
	public List<String> refIDs = new LinkedList<String>();

	public Map<String, String> toMap() {
		return MapUtil.genericMap("t", title, "y", year, "ab", abstracts);
	}

	public void addAuthor(String author) {
		authors.add(author);
	}

	public void addRef(String refID) {
		refIDs.add(refID);
	}
	@Override
	public String toString() {
		return "DBLPPaper [id=" + id + ", title=" + title + ", author="
				+ authors + ", year=" + year + ", venue=" + venue
				+ ", abstracts=" + abstracts + ", refID=" + refIDs + "]";
	}
}

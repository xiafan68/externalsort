package index;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import xiafan.file.FileUtil;
import xiafan.util.LatencyTracker;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * 接口 1. put(sentence, element) 将一个sentence分词进行索引 2. remove(sentence, element)
 * 将一个sentence分词，将对应项里面的element删除 3. retrieve(query) 将query分词后检索结果
 * 
 * element的实现约束： 1. TupleBinding的提供 2. Comparator类的提供
 * 
 * @author xiafan
 * 
 * @param <PostElement>
 */
public class InvertedIndex<PostElement> {
	private static Logger logger = LoggerFactory.getLogger(InvertedIndex.class);
	DbEnv env;
	private LatencyTracker segLatencyTracker = new LatencyTracker();
	private LatencyTracker retrievelLatencyTracker = new LatencyTracker();
	private LatencyTracker putLatencyTracker = new LatencyTracker();

	public InvertedIndex(File envHome_, TupleBinding binding_,
			Class<Comparator<byte[]>> nodeCompClass_) {
		env = new DbEnv(envHome_, binding_, nodeCompClass_);
	}

	public void init() {
		env.setup(false, true, 1024 * 1024 * 512);
	}

	public Map<String, List<PostElement>> search(String keywords)
			throws IOException {
		long startTime = System.currentTimeMillis();
		IKSegmenter segmenter = new IKSegmenter(new StringReader(keywords),
				true);
		Lexeme token = null;
		List<String> words = new LinkedList<String>();
		while (null != (token = segmenter.next())) {
			words.add(token.getLexemeText());
		}
		segLatencyTracker.addMicro(System.currentTimeMillis() - startTime);
		startTime = System.currentTimeMillis();
		Map<String, List<PostElement>> ret = null;
		ret = search(words.toArray(new String[words.size()]));
		retrievelLatencyTracker
				.addMicro(System.currentTimeMillis() - startTime);
		return ret;
	}

	public Map<String, List<PostElement>> search(String[] keywords) {
		Map<String, List<PostElement>> ret = new HashMap<String, List<PostElement>>();

		DatabaseEntry theData = new DatabaseEntry();
		DatabaseEntry theKey;
		Cursor cursor = env.getNodeDb().openCursor(null, null);
		// Pair<Integer, ByteBuffer> oKeyPair = deComposeKey(Arrays.copyOf(
		// theKey.getData(), theKey.getData().length));
		try {
			for (String keyword : keywords) {
				theKey = new DatabaseEntry(keyword.getBytes());
				List<PostElement> postList = new LinkedList<PostElement>();

				OperationStatus retVal = cursor.getSearchKey(theKey, theData,
						LockMode.DEFAULT);
				while (retVal == OperationStatus.SUCCESS) {
					postList.add((PostElement) env.getNodeBinding()
							.entryToObject(theData));

					retVal = cursor.getNextDup(theKey, theData,
							LockMode.DEFAULT);
				}
				ret.put(keyword, postList);
			}
		} catch (Exception ex) {
			logger.error(ex.toString());
		} finally {
			cursor.close();
		}
		return ret;
	}

	public void put(String keywords, PostElement element) throws IOException {
		long startTime = System.currentTimeMillis();
		IKSegmenter segmenter = new IKSegmenter(new StringReader(keywords),
				true);
		Lexeme token = null;
		List<String> terms = new LinkedList<String>();
		while (null != (token = segmenter.next())) {
			terms.add(token.getLexemeText());
		}
		segLatencyTracker.addMicro(System.currentTimeMillis() - startTime);
		for (String term : terms)
			put_intern(term, element);
	}

	public void put_intern(String keyword, PostElement element) {
		long startTime = System.currentTimeMillis();
		Cursor cursor = env.getNodeDb().openCursor(null, null);
		OperationStatus retVal = null;
		DatabaseEntry theData = new DatabaseEntry();
		DatabaseEntry theKey = new DatabaseEntry(keyword.getBytes());
		env.getNodeBinding().objectToEntry(element, theData);
		if (cursor.getSearchBoth(theKey, theData, LockMode.DEFAULT) == OperationStatus.NOTFOUND)
			retVal = cursor.put(theKey, theData);

		cursor.close();

		putLatencyTracker.addMicro(System.currentTimeMillis() - startTime);
	}

	public void close() {
		env.close();
	}

	public static void main(String[] args) throws IOException {
		FileUtil.delete("test1");
		InvertedIndex<NodeIDPostElement> index = new InvertedIndex<NodeIDPostElement>(
				new File("test1"),
				NodeIDPostElement.binding,
				(Class<Comparator<byte[]>>) NodeIDPostElement.NodeComparator.class
						.asSubclass(Comparator.class));
		index.init();
		index.put("test", new NodeIDPostElement(1));
		index.put("i am just a test", new NodeIDPostElement(2));
		System.out.println(index.search(new String[] { "test" }));
		System.out.println(index.search(new String[] { "am" }));
	}

	public void printPerformance() {
		System.out.println(String.format(
				"recent put:%f;\nrecent seg:%f;\nrecent retrieve:%f\n",
				putLatencyTracker.getRecentLatencyMicros(),
				segLatencyTracker.getRecentLatencyMicros(),
				retrievelLatencyTracker.getRecentLatencyMicros()));
	}

	public void flush() {
		env.flush();
	}
}

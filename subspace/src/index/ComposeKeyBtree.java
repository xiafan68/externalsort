/**
 * @author chengchengyu
 * 需要修改回去为简单的key
 */
package index;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.TreeMap;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import xiafan.file.FileUtil;
import xiafan.util.LatencyTracker;

import com.sleepycat.bind.tuple.DoubleBinding;
import com.sleepycat.bind.tuple.FloatBinding;
import com.sleepycat.bind.tuple.IntegerBinding;
import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.je.Cursor;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.EnvironmentMutableConfig;
import com.sleepycat.je.LockMode;
import com.sleepycat.je.OperationStatus;

/**
 * This class use the Berkeley DB to implement to IBtree interface.
 */
public class ComposeKeyBtree<PostElement> {
	private static Logger logger = LoggerFactory
			.getLogger(ComposeKeyBtree.class);

	private LatencyTracker putLatencyTracker = new LatencyTracker();
	private LatencyTracker retrievelLatencyTracker = new LatencyTracker();
	private LatencyTracker removeLatencyTracker = new LatencyTracker();

	// ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
	DbEnv dbenv;

	public DbEnv getDbenv() {
		return dbenv;
	}

	public void setDbenv(DbEnv dbenv) {
		this.dbenv = dbenv;
	}

	String dbpath;

	private int cacheSize;

	public ComposeKeyBtree(String dbpath, int cacheSize) {
		this.dbpath = dbpath;
		this.cacheSize = cacheSize;
		/*
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		try {

			String regPath = null;
			if (dbpath.lastIndexOf(":") < 0)
				regPath = dbpath;
			else
				regPath = dbpath.substring(dbpath.indexOf(':') + 1);
			mbs.registerMBean(this, new ObjectName(
					"imc.disxmldb.index:type=Btree," + "dbPath=" + regPath));
		} catch (Exception e) {
			// throw new RuntimeException(e);
		}
		*/
	}

	private void unRegister() {
		MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
		try {

			String regPath = null;
			if (dbpath.lastIndexOf(":") < 0)
				regPath = dbpath;
			else
				regPath = dbpath.substring(dbpath.indexOf(':') + 1);
			mbs.unregisterMBean(new ObjectName("imc.disxmldb.index:type=Btree,"
					+ "dbPath=" + regPath));
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	public void init(TupleBinding binding_,
			Class<Comparator<byte[]>> nodeCompClass_) {
		this.dbenv = new DbEnv(new File(dbpath), binding_, nodeCompClass_);
		dbenv.setup(false, true, 1024 * 1024 * 1);
	}

	public void close() {
		this.dbenv.flush();
		this.dbenv.close();
		// unRegister();
	}

	private static final String ENCODE = "UTF-8";

	public void put(String key, PostElement node) {
		DatabaseEntry theKey = composeKey(key);
		put(theKey, node);
	}

	public void put(DatabaseEntry theKey, PostElement node) {
		long startTime = System.currentTimeMillis();
		DatabaseEntry theData = new DatabaseEntry();

		Cursor cursor = dbenv.getNodeDb().openCursor(null, null);
		OperationStatus retVal = null;

		try {
			dbenv.getNodeBinding().objectToEntry(node, theData);
			/*
			 * dbenv.getNodeBinding().objectToEntry( new
			 * NodeUnit(node.getNodeID(), node.getRange(), node.getLevel()),
			 * theData);
			 */
			if (cursor.getSearchBoth(theKey, theData, LockMode.DEFAULT) == OperationStatus.NOTFOUND)
				retVal = cursor.put(theKey, theData);
		} catch (Exception dbe) {
			try {
				System.out.println("Error putting entry " + theKey.toString());
			} catch (Exception e) {
				e.printStackTrace();
			}
			// txn.abort();
		} finally {
			if (cursor != null) {
				cursor.close();
				cursor = null;
			}
		}
		// txn.commit();
		putLatencyTracker.addMicro(System.currentTimeMillis() - startTime);
	}

	/**
	 * the List<IndexArrayUnit> should be ordered by docID, NodeUnit.range[0],
	 * NodeUnit.range[1]
	 * 
	 * @param text
	 * @return
	 */

	public List<PostElement> get(String key) {
		DatabaseEntry theKey = new DatabaseEntry();
		try {
			theKey.setData(key.getBytes(ENCODE));
		} catch (UnsupportedEncodingException e) {
			theKey.setData(key.getBytes());
		}
		return get(theKey);
	}

	public List<PostElement> get(DatabaseEntry theKey) {
		long startTime = System.currentTimeMillis();
		List<PostElement> ret = new LinkedList<PostElement>();
		DatabaseEntry theData = new DatabaseEntry();

		Cursor cursor = dbenv.getNodeDb().openCursor(null, null);
		// Pair<Integer, ByteBuffer> oKeyPair = deComposeKey(Arrays.copyOf(
		// theKey.getData(), theKey.getData().length));

		try {
			OperationStatus retVal = cursor.getSearchKey(theKey, theData,
					LockMode.DEFAULT);

			while (retVal == OperationStatus.SUCCESS) {
				PostElement myNode = (PostElement) dbenv.getNodeBinding()
						.entryToObject(theData);
				ret.add(myNode);
				retVal = cursor.getNextDup(theKey, theData, LockMode.DEFAULT);
			}
		} finally {
			cursor.close();
			cursor = null;
			retrievelLatencyTracker.addMicro(System.currentTimeMillis()
					- startTime);
		}
		return ret;
	}

	/*
	public TreeMap<Integer, List<NodeUnit>> getLesser(String key
		) {
		return getLesserIntern(key, false);
	}

	public TreeMap<Integer, List<NodeUnit>> getLesserOrEqual(String key,
			IXMLFilter filter) {
		return getLesserIntern(key, filter, true);
	}

	public TreeMap<Integer, List<NodeUnit>> getLesserIntern(String key,
			IXMLFilter filter, boolean inclusion) {
		long startTime = System.currentTimeMillis();

		TreeMap<Integer, List<NodeUnit>> imap = new TreeMap<Integer, List<NodeUnit>>();

		DatabaseEntry theData = new DatabaseEntry();

		DatabaseEntry foundKey = composeKey(key, -1);
		ByteBuffer byteKey = validator.fromString(key);
		Cursor cursor = dbenv.getNodeDb().openCursor(null, null);
		LockMode lockMode = LockMode.DEFAULT;
		try {
			OperationStatus retVal = cursor.getSearchKeyRange(foundKey,
					theData, LockMode.DEFAULT);
			// 返回false只有可能当前key是所有索引项里面最大的
			if (retVal != OperationStatus.SUCCESS) {
				retVal = cursor.getLast(foundKey, theData, lockMode);
			}

			while (retVal == OperationStatus.SUCCESS) {
				Pair<Integer, ByteBuffer> nKeyPair = deComposeKey(foundKey
						.getData());
				if (!inclusion) {
					if (validator.compare(byteKey, nKeyPair.right) == 0)
						retVal = cursor.getPrevNoDup(foundKey, theData,
								lockMode);
					inclusion = true;
					continue;
				}
				if (validator.compare(byteKey, nKeyPair.right) < 0) {
					retVal = cursor.getPrevNoDup(foundKey, theData, lockMode);
					continue;
				}

				Node myNode = (Node) dbenv.getNodeBinding().entryToObject(
						theData);
				if (!filter.filter(myNode)) {
					List<NodeUnit> ln = imap.get(myNode.getXmlDocID());
					if (ln != null) {
						NodeUnit tempNode = new NodeUnit(myNode);
						int index = Collections.binarySearch(ln, tempNode,
								ComparatorByRangeFirst.instance);
						if (index < 0)
							ln.add(Math.abs(index) - 1, tempNode);
					} else {
						ln = new ArrayList<NodeUnit>();
						ln.add(new NodeUnit(myNode.getNodeID(), myNode
								.getRange(), myNode.getLevel()));
						imap.put(myNode.getXmlDocID(), ln);
					}
				}
				retVal = cursor.getPrev(foundKey, theData, LockMode.DEFAULT);
			}

			
		} catch (Exception willNeverOccur) {
		} finally {
			cursor.close();
			cursor = null;
			retrievelLatencyTracker.addMicro(System.currentTimeMillis()
					- startTime);
		}

		return imap;
	}

	public TreeMap<Integer, List<NodeUnit>> getGreater(String key) {
		return getGreater(key, IdentityXMLFilter.instance);
	}

	public TreeMap<Integer, List<NodeUnit>> getGreater(String key,
			IXMLFilter filter) {
		return getGreaterIntern(key, filter, false);
	}

	public TreeMap<Integer, List<NodeUnit>> getGreaterOrEqual(String key) {
		return getGreaterOrEqual(key, IdentityXMLFilter.instance);
	}

	public TreeMap<Integer, List<NodeUnit>> getGreaterOrEqual(String key,
			IXMLFilter filter) {
		return getGreaterIntern(key, filter, true);
	}

	private TreeMap<Integer, List<NodeUnit>> getGreaterIntern(String key,
			IXMLFilter filter, boolean inclusion) {
		long startTime = System.currentTimeMillis();

		TreeMap<Integer, List<NodeUnit>> imap = new TreeMap<Integer, List<NodeUnit>>();

		DatabaseEntry theData = new DatabaseEntry();

		DatabaseEntry foundKey = composeKey(key, -1);
		ByteBuffer byteKey = validator.fromString(key);

		Cursor cursor = dbenv.getNodeDb().openCursor(null, null);
		int temp = -1;

		try {
			// 首先找到第一个大于等于key的索引项,有可能返回的索引项不是当前key的，那么只需要直接退出。如果直接没找到，那么直接返回空结果
			OperationStatus retVal = cursor.getSearchKeyRange(foundKey,
					theData, LockMode.DEFAULT);

			while (retVal == OperationStatus.SUCCESS) {
				if (!inclusion) {
					int comp = 0;
					// 如果不需要包含key对应的索引列表，那么需要skip掉key对应的所有索引列表
					Pair<Integer, ByteBuffer> nKeyPair = deComposeKey(foundKey
							.getData());
					comp = validator.compare(byteKey, nKeyPair.right);
					if (comp == 0) {
						retVal = cursor.getNextNoDup(foundKey, theData,
								LockMode.DEFAULT);
					}
					inclusion = true;
					continue;
				}

				// 读取完一个索引项和一个文档id对应的索引列表
				Node myNode = (Node) dbenv.getNodeBinding().entryToObject(
						theData);
				if (!filter.filter(myNode)) {
					List<NodeUnit> ln = imap.get(myNode.getXmlDocID());
					if (ln != null) {
						NodeUnit tempNode = new NodeUnit(myNode);
						int index = Collections.binarySearch(ln, tempNode,
								ComparatorByRangeFirst.instance);
						if (index < 0)
							ln.add(Math.abs(index) - 1, tempNode);
					} else {
						ln = new ArrayList<NodeUnit>();
						ln.add(new NodeUnit(myNode.getNodeID(), myNode
								.getRange(), myNode.getLevel()));
						imap.put(myNode.getXmlDocID(), ln);
					}
				}
				retVal = cursor.getNext(foundKey, theData, LockMode.DEFAULT);
			}

		} finally {
			cursor.close();
			cursor = null;
			retrievelLatencyTracker.addMicro(System.currentTimeMillis()
					- startTime);
		}

		return imap;
	}
	*/
	public void remove(String key, NodeIDPostElement node) {
		DatabaseEntry theKey = composeKey(key);
		remove(theKey, node);
	}

	public void remove(String key) {
		long startTime = System.currentTimeMillis();
		Cursor cursor = dbenv.getNodeDb().openCursor(null, null);
		DatabaseEntry theKey = composeKey(key);
		DatabaseEntry theData = new DatabaseEntry();
		try {
			OperationStatus ops = cursor.getSearchKey(theKey, theData,
					LockMode.DEFAULT);
			while (ops == OperationStatus.SUCCESS) {
				cursor.delete();
				ops = cursor.getNextDup(theKey, theData, LockMode.DEFAULT);
			}
		} catch (DatabaseException dbe) {
			try {
				System.out.println("Error removing entry " + theKey.toString());
			} catch (Exception willNeverOccur) {
			}
			// txn.abort();
			throw dbe;
		} finally {
			cursor.close();
			cursor = null;
		}
		// txn.commit();
		removeLatencyTracker.addMicro(System.currentTimeMillis() - startTime);
	}

	public void remove(DatabaseEntry theKey, NodeIDPostElement node) {
		long startTime = System.currentTimeMillis();
		// Transaction txn = dbenv.getEnv().beginTransaction(null, null);

		DatabaseEntry theData = new DatabaseEntry();

		// Cursor cursor = dbenv.getNodeDb().openCursor(txn, null);
		Cursor cursor = dbenv.getNodeDb().openCursor(null, null);
		try {
			dbenv.getNodeBinding().objectToEntry(node, theData);
			OperationStatus ops = cursor.getSearchBoth(theKey, theData,
					LockMode.DEFAULT);
			if (ops == OperationStatus.SUCCESS) {
				cursor.delete();
			} else {
				System.out.println("deleting failed" + ops.toString());
			}
		} catch (DatabaseException dbe) {
			try {
				System.out.println("Error removing entry " + theKey.toString());
			} catch (Exception willNeverOccur) {
			}
			// txn.abort();
			throw dbe;
		} finally {
			cursor.close();
			cursor = null;
		}
		// txn.commit();
		removeLatencyTracker.addMicro(System.currentTimeMillis() - startTime);
	}

	public void adjustCache(long cacheSize) {
		EnvironmentMutableConfig conf = new EnvironmentMutableConfig();
		conf.setCacheSize(cacheSize);
		dbenv.getEnv().setMutableConfig(conf);
	}

	public void flush() {
		dbenv.flush();
	}

	public long getNCacheMiss() {
		return dbenv.getEnv().getStats(null).getNCacheMiss();
	}

	public long getTotalCacheSize() {
		return dbenv.getEnv().getStats(null).getCacheTotalBytes();
	}

	public double getAvgRetrievelLatency() {
		long n = retrievelLatencyTracker.getOpCount();
		long latency = retrievelLatencyTracker.getTotalLatencyMicros();
		if (n == 0) {
			return 0;
		}
		return (float) latency / (float) n;
	}

	public double getRecentRetrievelLatency() {
		return retrievelLatencyTracker.getRecentLatencyMicros();
	}

	public double getAvgRemoveLatency() {
		long n = removeLatencyTracker.getOpCount();
		long latency = removeLatencyTracker.getTotalLatencyMicros();
		if (n == 0) {
			return 0;
		}
		return (float) latency / (float) n;
	}

	public double getRecentRemoveLatency() {
		return removeLatencyTracker.getRecentLatencyMicros();
	}

	public double getAvgPutLatency() {
		long n = putLatencyTracker.getOpCount();
		long latency = putLatencyTracker.getTotalLatencyMicros();
		if (n == 0) {
			return 0;
		}
		return (float) latency / (float) n;
	}

	public double getRecentPutLatency() {
		return putLatencyTracker.getRecentLatencyMicros();
	}

	public long estimateKeyResultNumB(String key, int replicas) {
		assert replicas > 0;
		DatabaseEntry theKey = new DatabaseEntry();
		Cursor cursor = null;
		try {
			theKey = composeKey(key);
			// ByteBuffer byteKey = validator.fromString(key);
			DatabaseEntry theData = new DatabaseEntry();
			cursor = dbenv.getNodeDb().openCursor(null, null);
			OperationStatus retVal = cursor.getSearchKey(theKey, theData,
					LockMode.DEFAULT);
			int count = 0;
			if (retVal == OperationStatus.SUCCESS)
				count = cursor.count();
			return count / replicas;
		} catch (Exception e) {
			// e.printStackTrace();
		} finally {
			cursor.close();
			cursor = null;
		}
		return 0;
	}

	public long estimateKeyResultNum(String key, int replicas) {
		assert replicas > 0;
		DatabaseEntry theKey = new DatabaseEntry();
		Cursor cursor = dbenv.getNodeDb().openCursor(null, null);
		try {
			theKey = composeKey(key);
			// ByteBuffer byteKey = validator.fromString(key);
			DatabaseEntry theData = new DatabaseEntry();
			OperationStatus retVal = cursor.getSearchKey(theKey, theData,
					LockMode.DEFAULT);
			long count = 0;
			if (retVal == OperationStatus.SUCCESS)
				count = cursor.countEstimate();
			return count / replicas;
		} catch (Exception e) {
			// e.printStackTrace();
		} finally {
			cursor.close();
			cursor = null;
		}
		return 0;
	}

	private DatabaseEntry composeKey(String key) {
		DatabaseEntry theKey = new DatabaseEntry();
		try {
			theKey.setData(key.getBytes(ENCODE));
		} catch (UnsupportedEncodingException e) {
			theKey.setData(key.getBytes());
		}
		return theKey;
	}

	/**
	 * 判断这个索引项是否出现在索引列表里
	 */

	public boolean contains(String key, NodeIDPostElement node) {
		DatabaseEntry theKey = composeKey(key);
		DatabaseEntry theData = new DatabaseEntry();

		dbenv.getNodeBinding().objectToEntry(node, theData);
		/*
		 * dbenv.getNodeBinding() .objectToEntry( new NodeUnit(node.getNodeID(),
		 * node.getRange(), node.getLevel()), theData);
		 */
		Cursor cursor = dbenv.getNodeDb().openCursor(null, null);
		try {
			OperationStatus retVal = cursor.getSearchBoth(theKey, theData,
					LockMode.DEFAULT);
			return retVal == OperationStatus.SUCCESS;
		} finally {
			cursor.close();
			cursor = null;
		}
	}

	public static void main(String[] args) {
		ComposeKeyBtree btree = new ComposeKeyBtree("data/bdbtest",
				1024 * 1024 * 1024);
		btree.init(
				NodeIDPostElement.binding,
				(Class<Comparator<byte[]>>) NodeIDPostElement.NodeComparator.class
						.asSubclass(Comparator.class));
		btree.put("test", new NodeIDPostElement(1));
		btree.put("test", new NodeIDPostElement(2));
		btree.put("test1", new NodeIDPostElement(1));
		System.out.println(btree.get("test"));
		btree.remove("test", new NodeIDPostElement(1));
		System.out.println(btree.get("test"));
		btree.close();
	}
}
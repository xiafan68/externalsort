package util;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PerformanceTracker {
	public static final String TOUCHED = "touched";
	public static final String EXPLORED = "explored";
	public static final String IINDEX_TIME = "retrieve inverted index";
	public static final String EXPLORE_TIME = "graph search algo cost";
	public static final String PRES_INDEX_TIME = "retrieve prestige cost";
	public static final String NEO_TIME = "retrieve neo4j cost";
	public static final String CONSTRUCT_GRAPH_TIME = "construct graph cost";
	public static final String FILL_GRAPH_TIME = "fill graph cost";

	public static PerformanceTracker instance = new PerformanceTracker();

	ThreadLocal<HashMap<String, Long>> localCounters = new ThreadLocal<HashMap<String, Long>>();
	ThreadLocal<HashMap<String, Long>> preCounters = new ThreadLocal<HashMap<String, Long>>();
	AtomicLong lengthSum = new AtomicLong(0);
	AtomicInteger counter = new AtomicInteger(0);

	public void startExplore() {
		if (localCounters.get() == null) {
			localCounters.set(new HashMap<String, Long>());
		}
	}

	public void incre(String field, long count) {
		if (!localCounters.get().containsKey(field)) {
			localCounters.get().put(field, count);
		} else {
			localCounters.get().put(field,
					localCounters.get().get(field) + count);
		}
	}

	public void finishExplore() {
		for (Entry<String, Long> entry : localCounters.get().entrySet()) {
			// System.out.println(String.format("%s:%d", entry.getKey(),
			// entry.getValue()));
			counter.incrementAndGet();
			lengthSum.addAndGet(entry.getValue());
		}
		preCounters.set(localCounters.get());
		localCounters.set(null);
	}

	public HashMap<String, Long> getCounters() {
		return preCounters.get();
	}

	public void print() {
		if (preCounters.get() != null)
			for (Entry<String, Long> entry : preCounters.get().entrySet()) {
				System.out.println(String.format("%s:%d", entry.getKey(),
						entry.getValue()));

			}
		System.out.println("average count: "
				+ (lengthSum.get() / (float) (counter.get())));
	}

	@Override
	public String toString() {
		StringBuffer buf = new StringBuffer();
		if (localCounters.get() != null) {
			for (Entry<String, Long> entry : localCounters.get().entrySet()) {
				buf.append(String.format("%s:%d;\n", entry.getKey(),
						entry.getValue()));

			}
		} else if (preCounters.get() != null) {
			for (Entry<String, Long> entry : preCounters.get().entrySet()) {
				buf.append(String.format("%s:%d;\n", entry.getKey(),
						entry.getValue()));

			}
		}

		buf.append("average count: "
				+ (lengthSum.get() / (float) (counter.get())));
		return buf.toString();
	}
}

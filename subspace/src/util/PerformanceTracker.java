package util;

import java.util.HashMap;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PerformanceTracker {
	public static final String TOUCHED = "touched";
	public static final String EXPLORED = "explored";

	public static PerformanceTracker instance = new PerformanceTracker();

	ThreadLocal<HashMap<String, Long>> localCounters = new ThreadLocal<HashMap<String, Long>>();
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
			System.out.println(String.format("%s:%d", entry.getKey(),
					entry.getValue()));
			counter.incrementAndGet();
			lengthSum.addAndGet(entry.getValue());
		}
		localCounters.set(null);
	}

	public void print() {
		System.out.println("average count: "
				+ (lengthSum.get() / (float) (counter.get())));
	}
}

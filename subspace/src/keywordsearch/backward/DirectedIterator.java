package keywordsearch.backward;

import java.util.Iterator;
import java.util.PriorityQueue;

import org.neo4j.graphdb.Direction;

import xiafan.util.Pair;

public class DirectedIterator implements Iterator<Pair<Long, Float>> {

	public DirectedIterator(PriorityQueue queue, Direction diection) {

	}
	
	public void updatePriority(long nodeID, float priority) {
		
	}
	
	@Override
	public boolean hasNext() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Pair<Long, Float> next() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void remove() {
		// TODO Auto-generated method stub

	}

}

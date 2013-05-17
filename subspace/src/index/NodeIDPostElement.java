package index;

import java.util.Comparator;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseEntry;

public class NodeIDPostElement {
	public static TupleBinding<NodeIDPostElement> binding = new NodeIDPostElementBind();
	long id;

	public NodeIDPostElement(long id_) {
		this.id = id_;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	@Override
	public String toString() {
		return Long.toString(id);
	}
	
	public static class NodeComparator implements Comparator<byte[]> {

		public NodeComparator() {
		}

		public int compare(byte[] o1, byte[] o2) {

			DatabaseEntry dbe1 = new DatabaseEntry(o1);
			DatabaseEntry dbe2 = new DatabaseEntry(o2);

			NodeIDPostElementBind nodeBinding = new NodeIDPostElementBind();

			NodeIDPostElement node1 = (NodeIDPostElement) nodeBinding
					.entryToObject(dbe1);
			NodeIDPostElement node2 = (NodeIDPostElement) nodeBinding
					.entryToObject(dbe2);

			return (int) (node1.id - node2.id);
		}
	}

	public static class NodeIDPostElementBind extends
			TupleBinding<NodeIDPostElement> {

		@Override
		public NodeIDPostElement entryToObject(TupleInput arg0) {
			return new NodeIDPostElement(arg0.readLong());
		}

		@Override
		public void objectToEntry(NodeIDPostElement arg0, TupleOutput arg1) {
			arg1.writeLong(arg0.id);
		}

	}
}

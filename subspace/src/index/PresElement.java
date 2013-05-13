package index;

import java.util.Comparator;

import com.sleepycat.bind.tuple.TupleBinding;
import com.sleepycat.bind.tuple.TupleInput;
import com.sleepycat.bind.tuple.TupleOutput;
import com.sleepycat.je.DatabaseEntry;

public class PresElement {
	public static TupleBinding<PresElement> binding = new PresElementBind();
	float pres;

	public PresElement(float pres_) {
		this.pres = pres_;
	}

	public float getId() {
		return pres;
	}

	public void setId(float pres) {
		this.pres = pres;
	}

	@Override
	public String toString() {
		return Float.toString(pres);
	}

	public static class NodeComparator implements Comparator<byte[]> {

		public NodeComparator() {
		}

		public int compare(byte[] o1, byte[] o2) {

			DatabaseEntry dbe1 = new DatabaseEntry(o1);
			DatabaseEntry dbe2 = new DatabaseEntry(o2);

			PresElementBind nodeBinding = new PresElementBind();

			PresElement node1 = nodeBinding.entryToObject(dbe1);
			PresElement node2 = nodeBinding.entryToObject(dbe2);

			return (int) (node1.pres - node2.pres);
		}
	}

	public static class PresElementBind extends TupleBinding<PresElement> {

		@Override
		public PresElement entryToObject(TupleInput arg0) {
			return new PresElement(arg0.readFloat());
		}

		@Override
		public void objectToEntry(PresElement arg0, TupleOutput arg1) {
			arg1.writeFloat(arg0.pres);
		}

	}
}
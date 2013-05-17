package test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
/**
 * 对于hashmap,如果插入数据的顺序不同，会由于解决冲突时的策略，导致最后遍历元素的时候遍历得到的顺序也不同。
 * 对于TreeMap，由于key是有序的，因此无论插入的顺序如何，最后key上的遍历得到的顺序是一直的
 * @author xiafan
 *
 */
public class HashMapOrderTest {
	public static void main(String[] args) throws ParseException {
		System.out.println(new SimpleDateFormat("yyyy MM-DD").parse("2012 05-01").getTime()/1000);
		Random rand = new Random();
		Map<Integer, Integer> mapping = new TreeMap<Integer, Integer>();
		Map<Integer, Integer> omapping = new TreeMap<Integer, Integer>();
		ArrayList<Integer> numbers = new ArrayList<Integer>();
		
		for (int i = 0; i < 100; i++) {
			int number = rand.nextInt();
			numbers.add(number);
			mapping.put(number, number);
		}
		
		Collections.shuffle(numbers);
		
		for (int i = 0; i < 100; i++) {
			omapping.put(numbers.get(i), numbers.get(i));
		}
		
		Iterator<Integer> aIter = mapping.keySet().iterator(), oIter = omapping.keySet().iterator();
		while (aIter.hasNext()) {
			Integer a = aIter.next();
			Integer o = oIter.next();
			if (!a.equals(o)) {
				System.err.println("insert order influence the traverse order of hashmap");
				//break;
			}
			System.out.println("a: " + a + " o:" + o);
		}
		
		String a = "-";
		String b = "123";
		System.out.println(a.compareTo(b));
	}
}

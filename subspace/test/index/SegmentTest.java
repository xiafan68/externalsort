package index;

import java.io.IOException;
import java.io.StringReader;

import org.junit.Test;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

public class SegmentTest {
	@Test
	public void test() throws IOException {
		IKSegmenter seg = new IKSegmenter(new StringReader(
				"this is a very important test"), true);
		Lexeme token = seg.next();
		while (token != null) {
			System.out.println(token.getLexemeText());
			token = seg.next();
		}
	}
}

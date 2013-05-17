package sort.externalsort;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class GenerateTestData {
	public static int testFileCount = 10;
	public static int recordsPerFile = 100000;
	public static String junkStr = "asdfasdflakjsdfleoiurqpowenasdnsd;fjkalsjdf;lasjdfljas;jdfpwi; ;lasjdf;la asdljf;";
	public static void main(String[] args) throws IOException {
		String outputDir = "./testData";
		File file = new File(outputDir);
		file.mkdir();
		
		for (int i = 0; i < testFileCount; i++) {
			File outFile = new File(file, "test" + i);
			BufferedWriter output = new BufferedWriter(new FileWriter(outFile));
			
			Random rand = new Random();
			for (int l = 0; l < recordsPerFile; l++) {
				StringBuffer record = new StringBuffer();
				record.append(Math.abs(rand.nextLong()));
				record.append(" ");
				record.append(junkStr);
				output.write(record.toString());
				output.newLine();
			}
			output.close();
		}
	}
}

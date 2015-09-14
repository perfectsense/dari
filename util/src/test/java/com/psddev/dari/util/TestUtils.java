package com.psddev.dari.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.junit.Ignore;

@Ignore
public class TestUtils {
	public static String loadSampleFile(String filename) throws IOException {
		InputStream in = TestUtils.class.getClassLoader().getResourceAsStream(filename);
		if(in == null) {
			throw new FileNotFoundException("The resource file could not be found: " + filename);
		}
		return getStringFromInputStream(in);
	}
	
	private static String getStringFromInputStream(InputStream is) throws IOException {
		BufferedReader br = null;
		StringBuilder sb = new StringBuilder();

		String line;
		try {
 
			br = new BufferedReader(new InputStreamReader(is));
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
 
		return sb.toString();
	}
}

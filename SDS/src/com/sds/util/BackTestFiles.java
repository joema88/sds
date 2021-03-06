package com.sds.util;

import java.io.*;
import java.util.*;

public class BackTestFiles {

	public static void main(String[] args) {
		processedStocks();
	}

	public static Hashtable processedStocks() {
		Hashtable all = new Hashtable();

		File folder = new File("/home/joma/share/test/simple/");

		// Implementing FilenameFilter to retrieve only txt files

		FilenameFilter cvsFileFilter = new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				if (name.endsWith(".csv")) {
					return true;
				} else {
					return false;
				}
			}
		};

		// Passing txtFileFilter to listFiles() method to retrieve only txt files

		File[] files = folder.listFiles(cvsFileFilter);

		for (File file : files) {
			String fileName = file.getName();
			String tag1 = "StrategyReports_";
			//try {
			String symbol = fileName.substring(tag1.length());
			String tag2 = "_";
			symbol = symbol.substring(0, symbol.indexOf(tag2));

			// StrategyReports_BIIB_Yellow.csv
			// System.out.println(file.getName());
			// System.out.println("Symbol "+symbol);
			all.put(symbol, symbol);
			//}catch(Exception ex) {
			//	ex.printStackTrace(System.out);
			//}
		}

		Enumeration en = all.keys();
		int loop = 0;
		while (en.hasMoreElements()) {
			System.out.println(en.nextElement().toString().strip());
			loop++;

		}

		System.out.println("Total symbol count " + loop);
		System.out.println("           ");
		System.out.println(" -------------------------------- ");
		return all;
	}
}

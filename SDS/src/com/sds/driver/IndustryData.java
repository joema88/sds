package com.sds.driver;

import java.io.File;
import java.io.FilenameFilter;

import com.sds.file.CSVReader;

public class IndustryData {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String path = "/home/joma/share/test/";
		String cDate = "2020-10-24";
		readLoadIndustryFiles(path, cDate);

	}

	public static void readLoadIndustryFiles(String path, String cDate) {
		File folder = new File(path);

		// Implementing FilenameFilter to retrieve only txt files

		FilenameFilter txtFileFilter = new FilenameFilter() {
			// @Override
			public boolean accept(File dir, String name) {
				if (name.endsWith(".csv")&&name.contains("Industry")) {
					return true;

				} else {
					return false;
				}
			}
		};

		// Passing txtFileFilter to listFiles() method to retrieve only txt files

		File[] files = folder.listFiles(txtFileFilter);

		for (int k = 0; k < files.length; k++) {
			String nextFile = files[k].getName();
			try {
				if (nextFile.indexOf(cDate) >= 0&&nextFile.contains("Industry")) {
					System.out.println("Processing 1..." + nextFile);
					CSVReader.uploadIndustryCSVtoDB(path, nextFile);
				}
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}
		}

	}
}

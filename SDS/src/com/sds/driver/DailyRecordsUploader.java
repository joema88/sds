package com.sds.driver;

import com.sds.file.CSVReader;
import com.sds.db.*;
import java.io.File;
import java.io.FilenameFilter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Calendar;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import com.sds.analysis.*;

public class DailyRecordsUploader {

	public static void processDailySummaryScore(String date) {
		try {
			int dateID = DB.getDateID(date);
			PreparedStatement stmnt = DB.getStockIDQueryStmnt();
			stmnt.setInt(1, dateID);
			ResultSet rs = stmnt.executeQuery();
			while (rs.next()) {
				int stockID = rs.getInt(1);
				PT9.processStockToday("", stockID, dateID);
				PreparedStatement stmnt1 = DB.getTYPDSumByStockIDStmnt();
				stmnt1.setInt(1, stockID);
				stmnt1.setInt(2, dateID - 4);
				stmnt1.setInt(3, dateID);
				ResultSet rs1 = stmnt1.executeQuery();
				if (rs1.next()) {
					int tealSum = rs1.getInt(1);
					int yellowSum = rs1.getInt(2);
					int pinkSum = rs1.getInt(3);

					int score = 2 * tealSum - 2 * yellowSum - 5 * pinkSum;
					PreparedStatement stmnt2 = DB.getSCUpdateStmnt();
					stmnt2.setInt(1, score);
					stmnt2.setInt(2, stockID);
					stmnt2.setInt(3, dateID);
					stmnt2.executeUpdate();
				}
				
				PreparedStatement yp10SumCalStmnt = DB.getYP10SumCalStmnt();
				yp10SumCalStmnt.setInt(1, stockID);
				yp10SumCalStmnt.setInt(2, dateID - 9);
				yp10SumCalStmnt.setInt(3, dateID);
				ResultSet rs3 = yp10SumCalStmnt.executeQuery();
				if (rs3.next()) {
					int yellowSum = rs3.getInt(1);
					int pinkSum = rs3.getInt(2);
					int YP10 = yellowSum + pinkSum ;
					
					PreparedStatement yp10SumUpdateStmnt = DB.getYP10SumUpdateStmnt();
					
					yp10SumUpdateStmnt.setInt(1, YP10);
					yp10SumUpdateStmnt.setInt(2, stockID);
					yp10SumUpdateStmnt.setInt(3, dateID);
					yp10SumUpdateStmnt.executeUpdate();

				}
               //process BT9 Bull pattern one
				OneBullPattern.processStock("", stockID, dateID);
				//find passing points
				OneBullPattern.findPassPoints("", stockID, true);
			}
		} catch (Exception ex) {
			System.out.println("Error at processDailySummaryScore...");
			ex.printStackTrace(System.out);
		}

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String path = "/home/joma/share/test/";
		boolean currentDateProcessOnly = true;
		String cDate = "2020-05-28";
		int dateCountToBeProcessed = 7;
		int loopCount = 0;
		long t1 = System.currentTimeMillis();

		do {
		    readLoadRecords(path, cDate);
			System.out.println("Process daily summary and PT9...");
			processDailySummaryScore(cDate);
			try {
				Thread.sleep(5000);
			} catch (Exception ex) {

			}
			cDate = getNextDateString(cDate);
			loopCount++;
		} while (!currentDateProcessOnly && loopCount <= dateCountToBeProcessed);

		DB.closeConnection();
		
		// DailyAnalysis.analyzeStockTrend("2020-05-22");
		long t2 = System.currentTimeMillis();

		System.out.println("Total time cost is minutes: " + (t2 - t1) / 1000.0f / 60.0f);

	}

	public static String getNextDateString(String preDay) {
		String nextDate = "";

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		try {
			Date date = sdf.parse(preDay);
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);

			cal.add(Calendar.DATE, 1);

			while ((cal.get(Calendar.DAY_OF_WEEK) == Calendar.SUNDAY)
					|| (cal.get(Calendar.DAY_OF_WEEK) == Calendar.SATURDAY)) { // or
				// sunday
				cal.add(Calendar.DATE, 1);

				System.out.println("WEEKEND");
			}

			nextDate = sdf.format(cal.getTime());
			System.out.println("next date is " + nextDate);

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
		return nextDate;
	}

	public static void readLoadRecords(String path, String cDate) {
		File folder = new File(path);

		// Implementing FilenameFilter to retrieve only txt files

		FilenameFilter txtFileFilter = new FilenameFilter() {
			// @Override
			public boolean accept(File dir, String name) {
				if (name.endsWith(".csv")) {
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
				if (nextFile.indexOf(cDate) >= 0) {
					System.out.println("Processing 1..." + nextFile);
					CSVReader.uploadCSVtoDB(path, nextFile);
				}
			} catch (Exception ex) {
				ex.printStackTrace(System.out);
			}
		}

	}

}

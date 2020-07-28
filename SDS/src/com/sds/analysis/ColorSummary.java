package com.sds.analysis;

import java.sql.*;

import com.sds.db.DB;

public class ColorSummary {

	private static PreparedStatement queryColorSumStmnt = null;
	private static PreparedStatement updateColorSumStmnt = null;
	private static PreparedStatement updateColorOMStmnt = null;
	private static PreparedStatement noColorSumStmnt = null;
    //DATEID = 8455, count = 5203, 2018/7/18 starting point
	private static int startDateID = 8455;
	private static void init() {
		queryColorSumStmnt = DB.getColorSumStmnt();
		updateColorSumStmnt = DB.getUpdateColorSumStmnt();
		updateColorOMStmnt = DB.getOMColorUpdateStmnt();
		noColorSumStmnt = DB.getNoColorSumStmnt();

	}

	public static void main(String[] args) {
		try {
		
			long t1 = System.currentTimeMillis();
			PreparedStatement allStocks = DB.getAllStockIDs();
			allStocks.setInt(1, 1);

			ResultSet rs = allStocks.executeQuery();
			int sc = 0;
			System.out.println("-----------Begin---------");
			while (rs.next()) {
				sc++;
				int stockID = rs.getInt(1);
				for(int k = startDateID; k<=8963; k++) {
					updateColorSummary(stockID,k);
					updateOMColorSummary(stockID,k);
				}
				long t2 = System.currentTimeMillis();
				System.out.println(sc+" total stocks processed, time cost "+(t2-t1)/(1000*60));
				Thread.sleep(4000);
			}
		}catch(Exception ex) {
			ex.printStackTrace(System.out);
		}

	}
	
	public static void updateOMColorSummary(int stockID, int dateId) {
		try {
			init();
			int tSum = 0;
			int ySum = 0;
			int pSum = 0;
			int nSum = 0;
			queryColorSumStmnt.setInt(1, stockID);
			queryColorSumStmnt.setInt(2, dateId-24);
			queryColorSumStmnt.setInt(3, dateId);
			
			ResultSet rs = queryColorSumStmnt.executeQuery();
			
			if(rs.next()) {
				tSum = rs.getInt(1);
				ySum = rs.getInt(2);
				pSum = rs.getInt(3);
				rs.close();
				rs = null;
			}
			
			noColorSumStmnt.setInt(1, stockID);
			noColorSumStmnt.setInt(2, dateId-24);
			noColorSumStmnt.setInt(3, dateId);
			
           ResultSet rs1 = noColorSumStmnt.executeQuery();
			
			if(rs1.next()) {
				nSum =  rs1.getInt(1);
				rs1.close();
				rs1 = null;
			}
			
			updateColorOMStmnt.setInt(1, tSum);
			updateColorOMStmnt.setInt(2, nSum);
			updateColorOMStmnt.setInt(3, ySum);
			updateColorOMStmnt.setInt(4, pSum);
			updateColorOMStmnt.setInt(5, stockID);
			updateColorOMStmnt.setInt(6, dateId);
			
			updateColorOMStmnt.executeUpdate();
		}catch(Exception ex) {
			
		}
	}
	
	public static void updateColorSummary(int stockID, int dateId) {
		try {
			init();
			int tSum = 0;
			int ySum = 0;
			int pSum = 0;
			int nSum = 0;
			int dSum = 0;
			queryColorSumStmnt.setInt(1, stockID);
			queryColorSumStmnt.setInt(2, startDateID);
			queryColorSumStmnt.setInt(3, dateId);
			
			ResultSet rs = queryColorSumStmnt.executeQuery();
			
			if(rs.next()) {
				tSum = rs.getInt(1);
				ySum = rs.getInt(2);
				pSum = rs.getInt(3);
				dSum = rs.getInt(4);
				rs.close();
				rs = null;
			}
			
			noColorSumStmnt.setInt(1, stockID);
			noColorSumStmnt.setInt(2, startDateID);
			noColorSumStmnt.setInt(3, dateId);
			
           ResultSet rs1 = noColorSumStmnt.executeQuery();
			
			if(rs1.next()) {
				nSum =  rs1.getInt(1);
				rs1.close();
				rs1 = null;
			}
			
			updateColorSumStmnt.setInt(1, tSum);
			updateColorSumStmnt.setInt(2, nSum);
			updateColorSumStmnt.setInt(3, ySum);
			updateColorSumStmnt.setInt(4, pSum);
			updateColorSumStmnt.setInt(5, dSum);
			updateColorSumStmnt.setInt(6, stockID);
			updateColorSumStmnt.setInt(7, dateId);
			
			updateColorSumStmnt.executeUpdate();
		}catch(Exception ex) {
			
		}
	}

}

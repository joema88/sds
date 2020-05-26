package com.sds.analysis;

import java.sql.*;

import com.sds.db.DB;

public class Summary {

   private static PreparedStatement queryStmnt = null;
   private static PreparedStatement typSumQueryStmnt = null;
   private static PreparedStatement scUpdateStmnt = null;
   
   public static void init() {
	   queryStmnt = DB.getSymbolDateIDQueryStmnt();
	   typSumQueryStmnt = DB.getTYPDSumQueryStmnt();
	   scUpdateStmnt = DB.getSCUpdateStmnt();
   }
   
   public static void processStock(String symbol) {
	   init();
		int stockID = DB.getSymbolID(symbol);
		try {
			queryStmnt.setString(1, "AAPL");
			ResultSet rs = queryStmnt.executeQuery();
			
			while(rs.next()) {
				int dateID = rs.getInt(1);
				typSumQueryStmnt.setString(1, "AAPL");
				typSumQueryStmnt.setInt(2,dateID-4);
				typSumQueryStmnt.setInt(3,dateID);
				ResultSet rs2 = typSumQueryStmnt.executeQuery();
				if(rs2.next()) {
					int tealSum = rs2.getInt(1);
					int yellowSum = rs2.getInt(2);
					int pinkSum = rs2.getInt(3);
					int sc5 = 2*tealSum - 2*yellowSum - 5*pinkSum;
					
					scUpdateStmnt.setInt(1, sc5);
					scUpdateStmnt.setInt(2, stockID);
					scUpdateStmnt.setInt(3, dateID);
					scUpdateStmnt.executeUpdate();
					
				}
			}
			
		}catch(Exception ex) {
			ex.printStackTrace(System.out);
		}
   }
	public static void main(String[] args) {
		

	}

}

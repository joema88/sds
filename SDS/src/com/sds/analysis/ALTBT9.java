package com.sds.analysis;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.sds.db.ALTBT9DB;
import com.sds.db.DB;

// A lot of times BT9 will come days or weeks quicker if we allow BT8 (continuously
//or not within 9 days to become BT9, thus to get an early entry and better price
//More risk? Maybe, but more such important points to be leveraged
//Rather than relax Teal condition, we instead look one level higher
//of course we need to test successful rate of such a decision
public class ALTBT9 {

	public static void main(String[] args) {
		findAltBT9("CRNC", -1);

	}

	private static PreparedStatement teal10QueryStmnt = null;
	private static PreparedStatement dateIDQueryStmnt = null;
	private static PreparedStatement altBT9Update = null;
	private static PreparedStatement resetBT9 = null;
	private static PreparedStatement dateStmnt = null;


	public static void init() {
		teal10QueryStmnt = ALTBT9DB.getALTBT9Stmnt();
		dateIDQueryStmnt = ALTBT9DB.getDateIDQuery();
		altBT9Update = ALTBT9DB.getAltBT9Update();
		resetBT9 = ALTBT9DB.resetAltBT9();
		dateStmnt = ALTBT9DB.getDateStmnt();

	}

	public static void findAltBT9(String symbol, int stockID) {
		init();
		if (symbol != null && symbol.length() > 0) {
			stockID = DB.getSymbolID(symbol);
		}
		try {
			resetBT9.setInt(1, stockID);
			resetBT9.executeUpdate();
			dateIDQueryStmnt.setInt(1, stockID);

			ResultSet rs1 = dateIDQueryStmnt.executeQuery();
			int totalAdded = 0;

			while (rs1.next()) {
				int dateId = rs1.getInt(1);

				teal10QueryStmnt.setInt(1, stockID);
				teal10QueryStmnt.setInt(2, dateId);
				teal10QueryStmnt.setInt(3, dateId + 9);

				ResultSet rs2 = teal10QueryStmnt.executeQuery();

				int tealSum = 0;
				int tealSumBegin = 0;
				int tealSumEnd = 0;
				if (rs2.next()) {
					tealSum = rs2.getInt(1);
					if (tealSum == 9) {
						// check the first 9 teal
						teal10QueryStmnt.setInt(1, stockID);
						teal10QueryStmnt.setInt(2, dateId);
						teal10QueryStmnt.setInt(3, dateId + 8);
						ResultSet rs3 = teal10QueryStmnt.executeQuery();
						if (rs3.next()) {
							tealSumBegin = rs3.getInt(1);
						}

						// check the last 9 teal
						teal10QueryStmnt.setInt(1, stockID);
						teal10QueryStmnt.setInt(2, dateId + 1);
						teal10QueryStmnt.setInt(3, dateId + 9);
						ResultSet rs4 = teal10QueryStmnt.executeQuery();
						if (rs4.next()) {
							tealSumEnd = rs4.getInt(1);
						}

					}
				}

				// only update if there is no continuous 9
				if (tealSum == 9 && tealSumBegin < 9 && tealSumEnd < 9) {
					altBT9Update.setInt(1, stockID);
					altBT9Update.setInt(2, dateId + 9);
					altBT9Update.executeUpdate();
					totalAdded++;
					for (int k = 0; k < 10; k++) {
						//jump over the next 10 to get fresh start
						rs1.next();
					}
					dateStmnt.setInt(1, dateId + 9);
					ResultSet rs5 = dateStmnt.executeQuery();
					if(rs5.next()) {
						System.out.println("Add AltBT9 at " + (dateId+9)+" "+rs5.getString(1));
					}
					
				}
			}

			System.out.println("Add AltBT9 total " + totalAdded);
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

}

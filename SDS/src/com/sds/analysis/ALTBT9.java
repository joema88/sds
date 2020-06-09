package com.sds.analysis;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import com.sds.db.ALTBT9DB;
import com.sds.db.TwoBullDB;
import com.sds.db.DB;

// A lot of times BT9 will come days or weeks quicker if we allow BT8 (continuously
//or not within 9 days to become BT9, thus to get an early entry and better price
//More risk? Maybe, but more such important points to be leveraged
//Rather than relax Teal condition, we instead look one level higher
//of course we need to test successful rate of such a decision
public class ALTBT9 {

	public static void main(String[] args) {
		String symbol = "SLS";
		findAltBT9(symbol, -1);
		markPassPoints(symbol, -1);
		

	}

	private static PreparedStatement teal10QueryStmnt = null;
	private static PreparedStatement dateIDQueryStmnt = null;
	private static PreparedStatement altBT9Update = null;
	private static PreparedStatement resetBT9 = null;
	private static PreparedStatement dateStmnt = null;
	private static PreparedStatement queryTeal = null;
	private static PreparedStatement queryHighLowPrice = null;
	private static PreparedStatement passPriceUpdate = null;
	private static PreparedStatement passPointUpdate = null;
	private static PreparedStatement normalBT9Query = null;
	private static PreparedStatement aptvQuery = null;
	private static PreparedStatement passPointsQuery = null;
	private static PreparedStatement passPointsUpdate = null;

	
	public static void init() {
		teal10QueryStmnt = ALTBT9DB.getALTBT9Stmnt();
		dateIDQueryStmnt = ALTBT9DB.getDateIDQuery();
		altBT9Update = ALTBT9DB.getAltBT9Update();
		resetBT9 = ALTBT9DB.resetAltBT9();
		dateStmnt = ALTBT9DB.getDateStmnt();
		queryTeal = ALTBT9DB.getTealQuery();
		queryHighLowPrice = TwoBullDB.getHighLowPrice();
		passPriceUpdate = ALTBT9DB.passPriceUpdate();
		passPointUpdate = ALTBT9DB.passPointUpdate();
		normalBT9Query = ALTBT9DB.normalBT9Query();
		aptvQuery = ALTBT9DB.aptvQuery();
		passPointsQuery = ALTBT9DB.passPointsQuery();
		passPointsUpdate = ALTBT9DB.passPointsUpdate();
	}

	public static void markPassPoints(String symbol, int stockID) {
		init();
		int dateId1 = 0;
		float aptv1 = 0.0f;
		int dateId2 = 0;
		float aptv2 = 0.0f;
		if (symbol != null && symbol.length() > 0) {
			stockID = DB.getSymbolID(symbol);
		}
		try {
			aptvQuery.setInt(1, stockID);
			ResultSet rs1 = aptvQuery.executeQuery();

			while (rs1.next()) {
				if (dateId1 == 0) {
					dateId1 = rs1.getInt(1);
					aptv1 = rs1.getFloat(2);
				} else if (dateId2 == 0) {
					dateId2 = rs1.getInt(1);
					aptv2 = rs1.getFloat(2);
				}

				if (dateId1 > 0 && dateId2 > 0) {
					passPointsQuery.setInt(1, stockID);
					passPointsQuery.setInt(2, dateId1);
					passPointsQuery.setInt(3, dateId2 + 10);
					passPointsQuery.setFloat(4, aptv1);

					ResultSet rs2 = passPointsQuery.executeQuery();
					int preDateId = 0;
					int totalPass = 0;
					while (rs2.next()) {
						int pdateId = rs2.getInt(1);
						if (preDateId == 0) {
							preDateId = pdateId;
							totalPass = 1;
						} else if (preDateId > 0) {
							if ((pdateId - preDateId) == 1) {
								preDateId = pdateId;
								totalPass++;
							} else {
								preDateId = pdateId;
								totalPass = 1;
							}
						}

						if (totalPass < 12) {
							passPointsUpdate.setInt(1, totalPass);
							passPointsUpdate.setInt(2, stockID);
							passPointsUpdate.setInt(3, pdateId);
							passPointsUpdate.executeUpdate();
						}
					} // end while

					dateId1 = dateId2;
					aptv1 = aptv2;
					dateId2 = 0;
					aptv2 = 0.0f;

				}
			}

			// query dateId2 forward
			passPointsQuery.setInt(1, stockID);
			passPointsQuery.setInt(2, dateId1);
			passPointsQuery.setInt(3, dateId1 + 10000);
			passPointsQuery.setFloat(4, aptv1);

			ResultSet rs3 = passPointsQuery.executeQuery();
			int preDateId = 0;
			int totalPass = 0;
			while (rs3.next()) {
				int pdateId = rs3.getInt(1);
				if (preDateId == 0) {
					preDateId = pdateId;
					totalPass = 1;
				} else if (preDateId > 0) {
					if ((pdateId - preDateId) == 1) {
						preDateId = pdateId;
						totalPass++;
					} else {
						preDateId = pdateId;
						totalPass = 1;
					}
				}

				if (totalPass < 12) {
					passPointsUpdate.setInt(1, totalPass);
					passPointsUpdate.setInt(2, stockID);
					passPointsUpdate.setInt(3, pdateId);
					passPointsUpdate.executeUpdate();
				}
			}
			
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
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
				teal10QueryStmnt.setInt(3, dateId + 8);

				ResultSet rs2 = teal10QueryStmnt.executeQuery();

				int tealSum = 0;
				if (rs2.next()) {
					tealSum = rs2.getInt(1);
					if (tealSum == 8) {

						// check the first teal, make sure not zero
						// as if zero then next teal (after 8 teal) could be 1
						// cause unnecessary duplicate
						queryTeal.setInt(1, stockID);
						queryTeal.setInt(2, dateId);

						ResultSet rs3 = queryTeal.executeQuery();

						int firstTeal = 0;
						int preTeal = -1;
						if (rs3.next()) {
							firstTeal = rs3.getInt(1);
						}

						queryTeal.setInt(1, stockID);
						queryTeal.setInt(2, dateId - 1);

						ResultSet rs4 = queryTeal.executeQuery();
						if (rs4.next()) {
							preTeal = rs4.getInt(1);
						}

						if (firstTeal == 1 && preTeal == 0) {

							altBT9Update.setInt(1, stockID);
							altBT9Update.setInt(2, dateId + 8);
							altBT9Update.executeUpdate();
							totalAdded++;
							// for (int k = 0; k < 8; k++) {
							// jump over the next 10 to get fresh start
							// rs1.next();
							// }

							dateStmnt.setInt(1, dateId + 8);
							ResultSet rs5 = dateStmnt.executeQuery();
							findPassPrice(stockID, dateId + 8);
							if (rs5.next()) {
								System.out.println("Add AltBT9 at " + (dateId + 8) + " " + rs5.getString(1));
							}
						}
					}
				}

			}
			System.out.println("Add AltBT9 total " + totalAdded + " For stockid " + stockID);
			normalBT9Query.setInt(1, stockID);
			ResultSet rs6 = normalBT9Query.executeQuery();
			if (rs6.next()) {
				System.out.println("Normal BT9 count over the same period of time is " + rs6.getInt(1));
			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void findPassPrice(int stockID, int dateId) {
		try {
			queryHighLowPrice.setInt(1, stockID);
			queryHighLowPrice.setInt(2, dateId - 5);
			queryHighLowPrice.setInt(3, dateId);

			ResultSet rs = queryHighLowPrice.executeQuery();

			if (rs.next()) {
				float maxClose = rs.getFloat(1);
				float passPrice = 1.05f * maxClose;

				passPriceUpdate.setFloat(1, passPrice);
				passPriceUpdate.setInt(2, stockID);
				passPriceUpdate.setInt(3, dateId);
				passPriceUpdate.executeUpdate();

			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

}

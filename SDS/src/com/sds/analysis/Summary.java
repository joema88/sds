package com.sds.analysis;

import java.sql.*;

import com.sds.db.DB;
import com.sds.db.TwoBullDB;

public class Summary {

	private static PreparedStatement queryStmnt = null;
	private static PreparedStatement typSumQueryStmnt = null;
	private static PreparedStatement scUpdateStmnt = null;
	private static PreparedStatement yp10SumCalStmnt = null;
	private static PreparedStatement yp10SumUpdateStmnt = null;
	private static PreparedStatement queryAllCX520 = null;
	private static PreparedStatement findPreviousCCX = null;
	private static PreparedStatement updateCCX = null;
	private static PreparedStatement updateBDCXZero = null;
	private static PreparedStatement queryLastDayCX520 = null;
	private static PreparedStatement SYPTStmnt = null;
	private static PreparedStatement SYPTUpdate = null;
	
	public static void init() {
		queryStmnt = DB.getSymbolDateIDQueryStmnt();
		typSumQueryStmnt = DB.getTYPDSumQueryStmnt();
		scUpdateStmnt = DB.getSCUpdateStmnt();
		yp10SumCalStmnt = DB.getYP10SumCalStmnt();
		yp10SumUpdateStmnt = DB.getYP10SumUpdateStmnt();
		queryAllCX520 = TwoBullDB.getCX520HistoryStmnt();
		queryLastDayCX520 = TwoBullDB.getLastDayCX520Stmnt();
		findPreviousCCX = TwoBullDB.getPreviousCCXStmnt();
		updateCCX = TwoBullDB.getCCXUpdateStmnt();
		updateBDCXZero = TwoBullDB.getBDCXUpdateZero();
		SYPTStmnt = DB.getSYPTStmnt();
		SYPTUpdate = DB.getSYPTUpdate();
	
	}

	public static void processDailyStocks(String date) {
		init();
		int dateID = DB.getDateID(date);

	}

	public static void processLastDayCCX(String symbol, int stockID) {
		init();
		if (symbol != null && symbol.length() > 0) {
			stockID = DB.getSymbolID(symbol);
		}
		try {
			queryLastDayCX520.setInt(1, stockID);
			ResultSet rs4 = queryLastDayCX520.executeQuery();
			if (rs4.next()) {
				int cx520 = rs4.getInt(1);
				int dateID = rs4.getInt(2);
				findPreviousCCX.setInt(1, stockID);
				findPreviousCCX.setInt(2, dateID);
				ResultSet rs5 = findPreviousCCX.executeQuery();
				if (rs5.next()) {
					int pccx = rs5.getInt(1);
					int pbdcx = rs5.getInt(2);
					int ccx = 0;
					if ((pccx > 0 && cx520 > 0) || (pccx < 0 && cx520 < 0)) {
						ccx = pccx + cx520;
						// if (pbdcx != 13 && pbdcx != 1 && pbdcx != -1) { // except this buy point
						// value, all rest val set
						// = 0
						// NOW WE HAVE THE BDW (MERGED RESULT OF BDCX, TAKE OUT 13 DAYS
						// ON BDCX COLUMN, MAYBE PUT ON BDW? HOW MANY DAYS TO BUY? 13??
						if (pbdcx != 1 && pbdcx != -1) {
							updateBDCXZero.setInt(1, stockID);
							updateBDCXZero.setInt(2, dateID - 1);
							updateBDCXZero.executeUpdate();

						}
						pbdcx = pccx + cx520;

					} else if ((pccx > 0 && cx520 < 0)) {
						if (pbdcx == 1) { // as pccx > 0
							// merge here instead in Bull Pattern Two merge causing trouble
							findPreviousCCX.setInt(1, stockID);
							findPreviousCCX.setInt(2, dateID - 1);
							ResultSet rs6 = findPreviousCCX.executeQuery();
							if (rs6.next()) {
								int ppccx = rs6.getInt(1);
								int ppbdcx = rs6.getInt(2); // this one <0
								pbdcx = ppccx + cx520 - pbdcx; // merge here
								ccx = cx520;

								// make pbdcx (=1) zero
								updateBDCXZero.setInt(1, stockID);
								updateBDCXZero.setInt(2, dateID - 1);
								updateBDCXZero.executeUpdate();
								// make ppbdcx zero
								updateBDCXZero.setInt(1, stockID);
								updateBDCXZero.setInt(2, dateID - 2);
								updateBDCXZero.executeUpdate();

							}

						} else {
							ccx = cx520;
							pbdcx = cx520;
						}
					} else if ((pccx < 0 && cx520 > 0)) {
						if (pbdcx == -1) { // as pccx < 0
							// merge here instead in Bull Pattern Two merge causing trouble
							findPreviousCCX.setInt(1, stockID);
							findPreviousCCX.setInt(2, dateID - 1);
							ResultSet rs7 = findPreviousCCX.executeQuery();
							if (rs7.next()) {
								int ppccx = rs7.getInt(1);
								int ppbdcx = rs7.getInt(2); // this one >0
								pbdcx = ppccx + cx520 - pbdcx; // merge here
								ccx = cx520;

								// make pbdcx (=1) zero
								updateBDCXZero.setInt(1, stockID);
								updateBDCXZero.setInt(2, dateID - 1);
								updateBDCXZero.executeUpdate();
								// make ppbdcx zero
								updateBDCXZero.setInt(1, stockID);
								updateBDCXZero.setInt(2, dateID - 2);
								updateBDCXZero.executeUpdate();

							}

						} else {
							ccx = cx520;
							pbdcx = cx520;
						}
					} else if (pccx == 0) {
						ccx = cx520;
						pbdcx = cx520;
					}

					updateCCX.setInt(1, ccx);
					updateCCX.setInt(2, pbdcx);
					updateCCX.setInt(3, stockID);
					updateCCX.setInt(4, dateID);
					updateCCX.executeUpdate();

				}

			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processCCXHistory(String symbol, int stockID) {
		init();
		if (symbol != null && symbol.length() > 0) {
			stockID = DB.getSymbolID(symbol);
		}
		try {
			queryAllCX520.setInt(1, stockID);
			ResultSet rs4 = queryAllCX520.executeQuery();
			while (rs4.next()) {
				int cx520 = rs4.getInt(1);
				int dateID = rs4.getInt(2);
				if (dateID == 8505) {
					System.out.println("Attention...");
				}
				findPreviousCCX.setInt(1, stockID);
				findPreviousCCX.setInt(2, dateID);
				ResultSet rs5 = findPreviousCCX.executeQuery();
				if (rs5.next()) {
					int pccx = rs5.getInt(1);
					int pbdcx = rs5.getInt(2);
					int ccx = 0;
					if ((pccx > 0 && cx520 > 0) || (pccx < 0 && cx520 < 0)) {
						ccx = pccx + cx520;
						// if (pbdcx != 13 && pbdcx != 1 && pbdcx != -1) { // except this buy point
						// value, all rest val set
						// = 0
						// NOW WE HAVE THE BDW (MERGED RESULT OF BDCX, TAKE OUT 13 DAYS
						// ON BDCX COLUMN, MAYBE PUT ON BDW? HOW MANY DAYS TO BUY? 13??
						if (pbdcx != 1 && pbdcx != -1) {
							updateBDCXZero.setInt(1, stockID);
							updateBDCXZero.setInt(2, dateID - 1);
							updateBDCXZero.executeUpdate();

						}
						pbdcx = pbdcx + cx520;

					} else if ((pccx > 0 && cx520 < 0)) {
						if (pbdcx == 1) { // as pccx > 0
							// merge here instead in Bull Pattern Two merge causing trouble
							findPreviousCCX.setInt(1, stockID);
							findPreviousCCX.setInt(2, dateID - 1);
							ResultSet rs6 = findPreviousCCX.executeQuery();
							if (rs6.next()) {
								// int ppccx = rs6.getInt(1);
								int ppbdcx = rs6.getInt(2); // this one <0
								pbdcx = ppbdcx + cx520 - pbdcx; // merge here
								ccx = cx520;

								// make pbdcx (=1) zero
								updateBDCXZero.setInt(1, stockID);
								updateBDCXZero.setInt(2, dateID - 1);
								updateBDCXZero.executeUpdate();
								// make ppbdcx zero
								updateBDCXZero.setInt(1, stockID);
								updateBDCXZero.setInt(2, dateID - 2);
								updateBDCXZero.executeUpdate();

							}

						} else {
							ccx = cx520;
							pbdcx = cx520;
						}
					} else if ((pccx < 0 && cx520 > 0)) {
						if (pbdcx == -1) { // as pccx < 0
							// merge here instead in Bull Pattern Two merge causing trouble
							findPreviousCCX.setInt(1, stockID);
							findPreviousCCX.setInt(2, dateID - 1);
							ResultSet rs7 = findPreviousCCX.executeQuery();
							if (rs7.next()) {
								// int ppccx = rs6.getInt(1);
								int ppbdcx = rs7.getInt(2); // this one >0
								pbdcx = ppbdcx + cx520 - pbdcx; // merge here
								ccx = cx520;

								// make pbdcx (=1) zero
								updateBDCXZero.setInt(1, stockID);
								updateBDCXZero.setInt(2, dateID - 1);
								updateBDCXZero.executeUpdate();
								// make ppbdcx zero
								updateBDCXZero.setInt(1, stockID);
								updateBDCXZero.setInt(2, dateID - 2);
								updateBDCXZero.executeUpdate();

							}

						} else {
							ccx = cx520;
							pbdcx = cx520;
						}
					} else if (pccx == 0) {
						ccx = cx520;
						pbdcx = cx520;
					}

					updateCCX.setInt(1, ccx);
					updateCCX.setInt(2, pbdcx);
					updateCCX.setInt(3, stockID);
					updateCCX.setInt(4, dateID);
					updateCCX.executeUpdate();

				}

			}
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processStock(String symbol, int stockID) {
		init();
		if (symbol != null && symbol.length() > 0) {
			stockID = DB.getSymbolID(symbol);
		}
		try {
			queryStmnt.setString(1, symbol);
			ResultSet rs = queryStmnt.executeQuery();

			while (rs.next()) {
				int dateID = rs.getInt(1);
				typSumQueryStmnt.setString(1, symbol);
				typSumQueryStmnt.setInt(2, dateID - 4);
				typSumQueryStmnt.setInt(3, dateID);
				ResultSet rs2 = typSumQueryStmnt.executeQuery();
				if (rs2.next()) {
					int tealSum = rs2.getInt(1);
					int yellowSum = rs2.getInt(2);
					int pinkSum = rs2.getInt(3);
					int sc5 = 2 * tealSum - 2 * yellowSum - 5 * pinkSum;

					scUpdateStmnt.setInt(1, sc5);
					scUpdateStmnt.setInt(2, stockID);
					scUpdateStmnt.setInt(3, dateID);
					scUpdateStmnt.executeUpdate();

				}

				yp10SumCalStmnt.setInt(1, stockID);
				yp10SumCalStmnt.setInt(2, dateID - 9);
				yp10SumCalStmnt.setInt(3, dateID);
				ResultSet rs3 = yp10SumCalStmnt.executeQuery();
				if (rs3.next()) {
					int yellowSum = rs3.getInt(1);
					int pinkSum = rs3.getInt(2);
					int YP10 = yellowSum + 2 * pinkSum;
					yp10SumUpdateStmnt.setInt(1, YP10);
					yp10SumUpdateStmnt.setInt(2, stockID);
					yp10SumUpdateStmnt.setInt(3, dateID);
					yp10SumUpdateStmnt.executeUpdate();

				}

			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void processAllYTPSum(int dateID) {
		try {
			SYPTStmnt.setInt(1, dateID);
			ResultSet rs1 = SYPTStmnt.executeQuery();
			if (rs1.next()) {
				int tealSum = rs1.getInt(1);
				int yellowSum = rs1.getInt(2);
				int pinkSum = rs1.getInt(3);

				int score = tealSum - yellowSum - 2 * pinkSum;

				SYPTUpdate.setInt(1, score);
				SYPTUpdate.setInt(2, dateID);
				SYPTUpdate.executeUpdate();

			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	public static void main(String[] args) {
		init();
		// String symbol = "CIEN";
		// processLastDayCCX(symbol, -1);
		for (int dateId = 1; dateId < 8933; dateId++) {
			processAllYTPSum(dateId);
			System.out.println("Done "+dateId);
		}
	}

}

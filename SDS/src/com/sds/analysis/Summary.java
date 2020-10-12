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
	private static PreparedStatement BOYSStmnt = null;
	private static PreparedStatement BOSYUpdate = null;
	private static PreparedStatement CBIUpdate = null;

	public static void init() {
		CBIUpdate = DB.getCBIUpdateStmnt();
		BOYSStmnt = DB.getBOYSStmnt();
		BOSYUpdate = DB.getBosyUpdateStmnt();
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

	public static void processBOSY(int dateID) {
		try {
			BOYSStmnt.setInt(1, dateID-5);
			BOYSStmnt.setInt(2, dateID);
			ResultSet rs1 = BOYSStmnt.executeQuery();
			//String query = "SELECT OS,OB,OY,BI FROM  DATES  WHERE DATEID > ? 
			// AND DATEID<= ? ORDER BY DATEID DESC";
			float[] OS = new float[3];
			float[] OB = new float[3];
			float[] OY = new float[3];
			float AY = 0.0f;
			float BAT = 0.0f;
			int k = 0;
			while (rs1.next()) {
				OS[k]	= rs1.getFloat(1);	
				OB[k]	= rs1.getFloat(2);	
				OY[k]	= rs1.getFloat(3);
				k++;
				if(k>=3)
					break;
				
			}
			
			int qualified = 0;
			//OS >35.0, OB >50, OS+OB>100.0, OB Condition after OS, 3 days AVG OY <70.0
			//Then Bull turn up point
			if(OS[2]>35.0f) {
				if((OS[2]+OB[0])>100.0f&&OB[0]>50.0f) {
					qualified=100;
					
				}
			}else if(OS[1]>35.0f) {
				if((OS[1]+OB[0])>100.0f&&OB[0]>50.0f) {
					qualified=100;
					
				}
			}
			
			BAT = OS[2]+OB[0];

			if((OS[1]+OB[0])>BAT) {
				BAT = OS[1]+OB[0];
			}
			
			AY = (OY[0]+OY[1]+OY[2])/3.0f;
			if(AY<70.0f) {
				qualified = qualified + 8;
			}
			//bear max decrease of all indicator of the two days
			float BI1 = (OB[0]-OB[1])-(OS[0]-OS[1])-(OY[0]-OY[1]);
			float BI2 = (OB[0]-OB[2])-(OS[0]-OS[2])-(OY[0]-OY[2]);
			float BI = BI2;
			if(BI1<BI2) {
				BI = BI1;
			}
			
			//String query = "UPDATE  DATES  SET BOSY= ?,AY=?, BAT=?, BI=? WHERE DATEID = ?";
			BOSYUpdate.setInt(1, qualified);
			BOSYUpdate.setFloat(2, AY);
			BOSYUpdate.setFloat(3, BAT);
			BOSYUpdate.setFloat(4, BI);
			BOSYUpdate.setInt(5, dateID);
			BOSYUpdate.executeUpdate();


		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

	
	public static void processCBI(int dateID) {
		try {
			BOYSStmnt.setInt(1, dateID-5);
			BOYSStmnt.setInt(2, dateID);
			ResultSet rs1 = BOYSStmnt.executeQuery();
			//String query = "SELECT OS,OB,OY,BI FROM  DATES  WHERE DATEID > ? 
			// AND DATEID<= ? ORDER BY DATEID DESC";
			float[] OS = new float[3];
			float[] OB = new float[3];
			float[] OY = new float[3];
			float[] BI = new float[3];
			float AY = 0.0f;
			float BAT = 0.0f;
			int k = 0;
			while (rs1.next()) {
				OS[k]	= rs1.getFloat(1);	
				OB[k]	= rs1.getFloat(2);	
				OY[k]	= rs1.getFloat(3);
				BI[k]   = rs1.getFloat(4);
				k++;
				if(k>=3)
					break;
				
			}
			
			float CBI = BI[0]-2.0f*BI[1]+BI[2];
			
			//String query = "UPDATE  DATES  SET CBI=? WHERE DATEID = ?";
			CBIUpdate.setFloat(1, CBI);
			CBIUpdate.setInt(2, dateID);
			CBIUpdate.executeUpdate();


		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

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
				int total = rs1.getInt(4);

				// int score = tealSum - yellowSum - 2 * pinkSum;

				// oversold factor to prepare long
				//float os = 100.0f * ((float) pinkSum / (float) total + (float) yellowSum / (3 * (float) total));
				float os = 100.0f * ((float) pinkSum / (float) total);
				float ob = ((float) tealSum / (float) total ) * 100.0f;
				float oy = ((float) yellowSum / (float) total ) * 100.0f;
				// System.out.println("pinkSum "+pinkSum+" yellowSum "+yellowSum+" total
				// "+total);
				// System.out.println("OS "+os+ " os2 "+os2);
				System.out.println(dateID + " ++ " + os + "   -- " + ob+"  -- "+oy+ "   sample count "+total);
				SYPTUpdate.setFloat(1, os);
				SYPTUpdate.setFloat(2, ob);
				SYPTUpdate.setFloat(3, oy);
				SYPTUpdate.setInt(4, total);
				SYPTUpdate.setInt(5, dateID);
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
		for (int dateId = 9017; dateId >1; dateId--) {
			// processAllYTPSum(dateId);
			//processAllYTPSum(dateId);
			//System.out.println("Done " + dateId);
			//processBOSY(dateId);
			processCBI(dateId);
			System.out.println("Done " + dateId);
		}
	}

}

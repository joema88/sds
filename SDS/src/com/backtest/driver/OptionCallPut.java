package com.backtest.driver;

import java.sql.PreparedStatement;
import com.sds.db.*;
import java.sql.*;

import com.sds.db.DB;

public class OptionCallPut {

	public static void main(String[] args) {
		try {
			float totalInput = 0.0f;
			float totalNow = 0.0f;

			long t1 = System.currentTimeMillis();
			PreparedStatement allStocks = DB.getAllStockIDs();
			PreparedStatement dBulls = DB.getDBulls();
			allStocks.setInt(1, 1);

			ResultSet rs = allStocks.executeQuery();
			int totalSC = 0;
			int totalCount = 0;
			float yield = 0.0f;
			int daysForOptions = 60; // 3 months
			float optionPremiumToPrice = 0.20f;
			String watchStock = "WST";
			System.out.println("-----------Begin---------");
			while (rs.next()) {
				int stockID = rs.getInt(1);
				// 7/17/2020
				// dBulls.setInt(1, 8958);
				dBulls.setInt(1, stockID);

				ResultSet rs2 = dBulls.executeQuery();

				float totalInput1 = 0.0f;
				float totalNow1 = 0.0f;
				int totalShares = 0;
				int preDateId = 0;
				int[] startIndex = new int[1000];
				float[] prices = new float[1000];
				int count = 0;
				while (rs2.next()) {
					int dateId = rs2.getInt(1);
					int pass = rs2.getInt(4);
					int apas = rs2.getInt(5);
					String symbol = rs2.getString(6);
					float price = rs2.getFloat(7);
					if (dateId < 8960) {
						if ((pass >= 1 && apas >= 1)) {
							if (symbol.equalsIgnoreCase(watchStock)) {
								System.out.println(symbol + " bought at " + dateId);

								startIndex[count] = dateId;
								prices[count] = price;
								count++;
								totalCount++;
							}
						}

						for (int k = 0; k < count; k++) {
							// if (symbol.equalsIgnoreCase("OSTK"))
							// System.out.println(symbol+ " price "+price+" prices["+k+"]"+prices[k]);
							if ((startIndex[k] + daysForOptions) > dateId) { // 3 months, assume 30% on 6 month premium
								if (prices[k] > 0.001f && (price > (2.0f * prices[k]))) {
									if (symbol.equalsIgnoreCase(watchStock)) {
										totalSC++;

										System.out.println(symbol + " Success trade between " + startIndex[k] + " and "
												+ dateId + " price doubled ");
										yield = yield + (price - prices[k] - 2.0f * optionPremiumToPrice * prices[k])
												/ (2.0f * optionPremiumToPrice * prices[k]);
										// System.out.println("Yield "+yield);
										prices[k] = -100.0f;
									}

								} else if (prices[k] > 0.001f && (price < 0.5f * prices[k])) {
									if (symbol.equalsIgnoreCase(watchStock)) {
										System.out.println(symbol + " Success trade between " + startIndex[k] + " and "
												+ dateId + " price halfed ");

										totalSC++;
										yield = yield + (prices[k] - price - 2.0f * optionPremiumToPrice * prices[k])
												/ (2.0f * optionPremiumToPrice * prices[k]);
										;

										prices[k] = -100.0f;
									}
									// yield = yield +500;
								}
							} else if ((startIndex[k] + daysForOptions) == dateId) {
								if (symbol.equalsIgnoreCase(watchStock)) {
									if (prices[k] > 0.001f && price > prices[k]) {
										yield = yield + (price - prices[k] - 2.0f * optionPremiumToPrice * prices[k])
												/ (2.0f * optionPremiumToPrice * prices[k]);
										prices[k] = -100.0f;
									} else if (prices[k] > 0.001f && price < prices[k]) {
										yield = yield + (prices[k] - price - 2.0f * optionPremiumToPrice * prices[k])
												/ (2.0f * optionPremiumToPrice * prices[k]);
										prices[k] = -100.0f;
									}
								}
							}

						}
					} else if (dateId == 8960) {
						// System.out.println("sTOCK ID "+symbol+" dateId "+dateId+" total "+totalInput1
						// );
						if (symbol.equalsIgnoreCase(watchStock)) {
							for (int k = 0; k < count; k++) {
								if (prices[k] > 0.001f && price > prices[k]) {
									yield = yield + (price - prices[k] - 2.0f * optionPremiumToPrice * prices[k])
											/ (2.0f * optionPremiumToPrice * prices[k]);
									prices[k] = -100.0f;
								} else if (prices[k] > 0.001f && price < prices[k]) {
									yield = yield + (prices[k] - price - 2.0f * optionPremiumToPrice * prices[k])
											/ (2.0f * optionPremiumToPrice * prices[k]);
									prices[k] = -100.0f;
								}
							}
							System.out.println("Total trade " + totalCount + " successful count " + totalSC);
							System.out.println("Yield " + yield + " actual yield " + 100.0f * yield / totalCount + "%");
						}

					}
				}

			}

			System.out.println("For all stocks total input " + totalInput + " total now " + totalNow);

			DB.closeConnection();
		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}

	}

}

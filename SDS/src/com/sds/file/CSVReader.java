package com.sds.file;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.Statement;

import com.sds.db.DB;

public class CSVReader {

	public static void main(String[] args) {

		String csvFile = "/home/joma/share/test/2020-05-19-watchlistUP.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";

		try {

			boolean start = false;
			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {
				if (line.indexOf("Last") > 0) {
					start = true;
				}
				// use comma as separator
				if (start) {
					String[] data = line.split(cvsSplitBy);
					String symbol = data[0];
					float percentage = 0.0f;
					try {
						String val = data[1].strip().substring(0, data[1].strip().length() - 1);
						// System.out.println("data[1] "+val);
						percentage = Float.parseFloat(val);
					} catch (Exception ex) {

					}
					float close = 0.0f;
					try {
						close = Float.parseFloat(data[2].strip());
					} catch (Exception ex) {

					}
					float netChange = 0.0f;
					try {
						netChange = Float.parseFloat(data[3].strip());
					} catch (Exception ex) {

					}
					float atr = 0.0f;
					try {
						atr = Float.parseFloat(data[4].strip());
					} catch (Exception ex) {

					}
					float open = 0.0f;
					try {
						open = Float.parseFloat(data[5].strip());
					} catch (Exception ex) {

					}
					float high = 0.0f;
					try {
						high = Float.parseFloat(data[6].strip());
					} catch (Exception ex) {

					}
					float low = 0.0f;
					try {
						low = Float.parseFloat(data[7].strip());
					} catch (Exception ex) {

					}
					float low52 = 0.0f;
					try {
						low52 = Float.parseFloat(data[8].strip());
					} catch (Exception ex) {

					}
					float high52 = 0.0f;
					try {
						high52 = Float.parseFloat(data[9].strip());
					} catch (Exception ex) {

					}
					// 119.88,"53,714 M","1,003,250"
					float marketCap = 0.0f;
					try {
						String val = line.substring(line.indexOf(data[9]) + data[9].length() + 2, line.indexOf(" M"));
						// System.out.println(symbol+ " data[10] "+val);

						marketCap = Float.parseFloat(val.replaceAll(",", ""));
					} catch (Exception ex) {

					}
					float volume = 0.0f;
					try {
						String val1 = line.substring(line.indexOf(" M") + 4);
						// System.out.println("val1 "+val1);
						if (val1.indexOf("\"") >= 0) {
							String val = line.substring(line.indexOf(" M") + 5, line.length() - 1);
							// System.out.println("vale "+val);
							volume = Float.parseFloat(val.replaceAll(",", ""));
						} else {
							volume = Float.parseFloat(val1);
						}
					} catch (Exception ex) {

					}

					// if (symbol.equalsIgnoreCase("BRK/A"))
					System.out.println(symbol + ": " + percentage + ": " + close + ": " + netChange + ": " + atr + ": "
							+ open + ": " + high + ": " + low + ": " + low52 + ": " + high52 + ": " + marketCap + ": "
							+ volume);
					// System.out.println("Country [code= " + country[4] + " , name=" + country[5] +
					// "]");
				}

			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

	public static boolean insertSymbol(String symbol, int ID, PreparedStatement stmnt) {
		boolean inserted = false;
		if (!DB.checkSymbolExist(symbol)) {
			try {
				stmnt.setInt(1, ID);
				stmnt.setString(2, symbol);
				stmnt.execute();

				inserted = true;
			} catch (Exception ex) {

			}
		}
		return inserted;
	}

	public static boolean insertDate(String dateStr, int ID, PreparedStatement stmnt) {
		boolean inserted = false;
		// java.sql.Date startDate = new java.sql.Date(dateStr);
		if (!DB.checkDateExist(dateStr)) {
			try {
				stmnt.setInt(1, ID);
				stmnt.setString(2, dateStr);
				stmnt.execute();

				inserted = true;
			} catch (Exception ex) {

			}
		}
		return inserted;
	}

	public static void updateBBRecord(int stockID, int dateID, String field, int val) {
		Statement stmnt = DB.getStatement();
		try {
			String query = " UPDATE BBROCK SET " + field + " = " + val + " WHERE STOCKID = " + stockID + " AND DATEID="
					+ dateID;
			stmnt.executeUpdate(query);

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}

	public static void uploadCSVtoDB(String path, String fileName) {
		String csvFile = path + fileName;
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";

		int nextDateID = DB.getNextDateID();
		int nextSymbolID = DB.getNextSymbolID();
		PreparedStatement symbolStmnt = DB.getSymbolInsertStatement();
		PreparedStatement dateStmnt = DB.getDateInsertStatement();
		PreparedStatement rockStmnt = DB.getRockInsertStatement();

		String dateStr = fileName.substring(0, fileName.indexOf("-watchlist"));

		if (insertDate(dateStr, nextDateID, dateStmnt)) {
			nextDateID++;
		}
		int currentDateID = DB.getDateID(dateStr);

		System.out.println(dateStr + ": " + currentDateID);
		try {

			boolean start = false;
			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {

				// use comma as separator
				if (start) {
					String[] data = line.split(cvsSplitBy);
					String symbol = data[0];
					if (insertSymbol(symbol, nextSymbolID, symbolStmnt)) {
						nextSymbolID++;
					}
					int stockID = DB.getSymbolID(symbol);

					boolean recordExists = DB.checkBBRecordExist(stockID, currentDateID);
					if (recordExists) {
						// update record only
						String fieldName = "TEAL";
						if (fileName.toLowerCase().indexOf("struggle") >= 0) {
							fieldName = "YELLOW";
						} else if (fileName.toLowerCase().indexOf("up") >= 0) {
							fieldName = "TEAL";
						} else if (fileName.toLowerCase().indexOf("down") >= 0) {
							fieldName = "PINK";
						}

						if (fileName.toLowerCase().indexOf("base") < 0) {
							updateBBRecord(stockID, currentDateID, fieldName, 1);
						}

					} else {
						float percentage = 0.0f;
						try {
							String val = data[1].strip().substring(0, data[1].strip().length() - 1);
							// System.out.println("data[1] "+val);
							percentage = Float.parseFloat(val);
						} catch (Exception ex) {

						}
						float close = 0.0f;
						try {
							close = Float.parseFloat(data[2].strip());
						} catch (Exception ex) {

						}
						float netChange = 0.0f;
						try {
							netChange = Float.parseFloat(data[3].strip());
						} catch (Exception ex) {

						}
						float atr = 0.0f;
						try {
							if (!data[4].strip().equalsIgnoreCase("NaN"))
								atr = Float.parseFloat(data[4].strip());
						} catch (Exception ex) {

						}
						float open = 0.0f;
						try {
							open = Float.parseFloat(data[5].strip());
						} catch (Exception ex) {

						}
						float high = 0.0f;
						try {
							high = Float.parseFloat(data[6].strip());
						} catch (Exception ex) {

						}
						float low = 0.0f;
						try {
							low = Float.parseFloat(data[7].strip());
						} catch (Exception ex) {

						}
						float low52 = 0.0f;
						try {
							low52 = Float.parseFloat(data[8].strip());
						} catch (Exception ex) {

						}
						float high52 = 0.0f;
						try {
							high52 = Float.parseFloat(data[9].strip());
						} catch (Exception ex) {

						}
						// 119.88,"53,714 M","1,003,250"
						// 714 M,"1,003,250"
						float marketCap = 0.0f;
						try {
							if (line.indexOf(" M\",") > 0) {
								String val = line.substring(line.indexOf(data[9]) + data[9].length() + 2,
										line.indexOf(" M\","));
								// System.out.println(symbol+ " data[10] "+val);

								marketCap = Float.parseFloat(val.replaceAll(",", ""));
							} else if (line.indexOf(" M,") > 0) {
								String val = line.substring(line.indexOf(data[9]) + data[9].length() + 1,
										line.indexOf(" M,"));
								// System.out.println(symbol+ " data[10] "+val);

								marketCap = Float.parseFloat(val.replaceAll(",", ""));

							}
						} catch (Exception ex) {

						}
						float volume = 0.0f;
						try {
							if (line.indexOf(" M\",") > 0) {
								String val1 = line.substring(line.indexOf(" M\",") + 4);
								// System.out.println("val1 "+val1);
								if (val1.indexOf("\"") >= 0) {
									String val = line.substring(line.indexOf(" M") + 5, line.length() - 1);
									// System.out.println("vale "+val);
									volume = Float.parseFloat(val.replaceAll(",", ""));
								} else if (val1 != null && val1.trim().length() > 0) {
									volume = Float.parseFloat(val1);
								}
							} else if (line.indexOf(" M,") > 0) {
								String val1 = line.substring(line.indexOf(" M") + 3);
								// System.out.println("val1 "+val1);
								if (val1.indexOf("\"") >= 0) {
									String val = line.substring(line.indexOf(" M") + 4, line.length() - 1);
									// System.out.println("vale "+val);
									volume = Float.parseFloat(val.replaceAll(",", ""));
								} else if (val1 != null && val1.trim().length() > 0) {
									volume = Float.parseFloat(val1);
								}
							}

						} catch (Exception ex) {
							ex.printStackTrace(System.out);
						}

						// insert records;
						try {
							// System.out.println(symbol + ": " + percentage + ": " + close + ": " +
							// netChange + ": " + atr
							// + ": " + open + ": " + high + ": " + low + ": " + low52 + ": " + high52 + ":
							// "
							// + marketCap + ": " + volume);
							rockStmnt.setInt(1, stockID);
							rockStmnt.setInt(2, currentDateID);
							rockStmnt.setFloat(3, percentage);
							rockStmnt.setFloat(4, close);
							rockStmnt.setFloat(5, netChange);
							rockStmnt.setFloat(6, atr);
							rockStmnt.setFloat(7, open);
							rockStmnt.setFloat(8, high);
							rockStmnt.setFloat(9, low);
							rockStmnt.setFloat(10, low52);
							rockStmnt.setFloat(11, high52);
							rockStmnt.setFloat(12, marketCap);
							rockStmnt.setFloat(13, volume);
							// yellow
							if (fileName.toLowerCase().indexOf("struggle") > 0) {
								rockStmnt.setInt(14, 1);
							} else {
								rockStmnt.setInt(14, 0);
							}
							// teal
							if (fileName.toLowerCase().indexOf("up") > 0) {
								rockStmnt.setInt(15, 1);
							} else {
								rockStmnt.setInt(15, 0);
							}
							// pink
							if (fileName.toLowerCase().indexOf("down") > 0) {
								rockStmnt.setInt(16, 1);
							} else {
								rockStmnt.setInt(16, 0);
							}

							rockStmnt.execute();
						} catch (Exception ex) {
							ex.printStackTrace(System.out);
						}
						// insert records;

					}
				}

				// kick start parsing after this
				if (line.indexOf("Last") > 0) {
					start = true;
				}
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}

	}

}
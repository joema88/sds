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
		csvFile = "/home/joma/share/test/2020-05-28-watchlistBase3.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";

		try {

			boolean start = false;
			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {

				// use comma as separator
				if (start) {
					String[] data = line.split(cvsSplitBy);
					String symbol = data[0];
					if (symbol.equalsIgnoreCase("AMOV")) {
						int abc = 0;
						abc++;
					}
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

					float open = 0.0f;
					try {
						open = Float.parseFloat(data[4].strip());
					} catch (Exception ex) {

					}
					float high = 0.0f;
					try {
						high = Float.parseFloat(data[5].strip());
					} catch (Exception ex) {

					}
					float low = 0.0f;
					try {
						low = Float.parseFloat(data[6].strip());
					} catch (Exception ex) {

					}
					float low52 = 0.0f;
					try {
						low52 = Float.parseFloat(data[7].strip());
					} catch (Exception ex) {

					}
					float high52 = 0.0f;
					try {
						high52 = Float.parseFloat(data[8].strip());
					} catch (Exception ex) {

					}
					// 119.88,"53,714 M","1,003,250"
					float marketCap = 0.0f;
					try {
						// System.out.println(line);
						// System.out.println("Split 8 " + data[8]);
						// System.out.println("Split 9 " + data[9]);
						if (line.indexOf(" M") > 0) {
							String val = line.substring(line.indexOf(data[8]) + data[8].length() + 1,
									line.indexOf(" M"));
							// System.out.println(symbol + " data[9] " + val);
							// System.out.println(val.replaceAll(",", "").replaceAll("\"", ""));
							marketCap = Float.parseFloat(val.replaceAll(",", "").replaceAll("\"", ""));
						} else {
							marketCap = Float.parseFloat(data[9].replaceAll(",", ""));
						}
					} catch (Exception ex) {

					}
					// System.out.println("Split 10 " + data[10]);
					float volume = 0.0f;
					float atr = 0.0f;

					// CETXW,-0.29%,.0349,-.0001,.0350,.0352,.0300,.0021,.1000,0,"3,600",0.0112
					if (line.indexOf(" M") < 0) {
						if (line.indexOf("\"") >= 0) {
							String val1 = line.substring(line.indexOf("\"") + 1);
							// System.out.println("vale1 " + val1);
							String val2 = val1.substring(0, val1.indexOf("\""));
							// System.out.println("vale2 " + val2);
							volume = Float.parseFloat(val2.replaceAll(",", ""));
							String val3 = val1.substring(val1.indexOf("\"") + 2, val1.length());
							// System.out.println("vale3 " + val3);
							if (!val3.equalsIgnoreCase("NaN"))
								atr = Float.parseFloat(val3);
						} else {
							volume = Float.parseFloat(data[10]);
							if (!data[11].equalsIgnoreCase("NaN"))
								atr = Float.parseFloat(data[11]);
						}
					} else {

						String val1 = line.substring(line.indexOf(" M") + 4);

						// System.out.println("val1 "+val1);
						if (val1.indexOf("\"") >= 0) {
							String val11 = line.substring(line.indexOf(" M") + 4);
							// System.out.println("vale1 " + val11);
							String val2 = val11.substring(0, val11.indexOf("\""));
							if (val2.strip().length() == 0) {
								val11 = line.substring(line.indexOf(" M") + 5);
								// System.out.println("vale1 " + val11);
								val2 = val11.substring(0, val11.indexOf("\""));
							}
							// System.out.println("vale2 " + val2);
							volume = Float.parseFloat(val2.replaceAll(",", ""));
							String val3 = val11.substring(val11.indexOf("\"") + 2, val11.length());
							// System.out.println("vale3 " + val3);
							if (!val3.equalsIgnoreCase("NaN"))
								atr = Float.parseFloat(val3);

						} else {
							String vStr = val1.substring(0, val1.indexOf(","));
							volume = Float.parseFloat(vStr);

							String val3 = val1.substring(val1.indexOf(",") + 1, val1.length());
							// System.out.println("vale3 " + val3);
							if (!val3.equalsIgnoreCase("NaN"))
								atr = Float.parseFloat(val3);

						}

					}

					// System.out.println("Split 11 " + data[11]);

					// if (symbol.equalsIgnoreCase("BRK/A"))
					System.out.println(symbol + ": " + percentage + ": " + close + ": " + netChange + ": " + open + ": "
							+ high + ": " + low + ": " + low52 + ": " + high52 + ": " + marketCap + ": " + volume + ": "
							+ atr);
					// System.out.println("Country [code= " + country[4] + " , name=" + country[5] +
					// "]");

				}
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
		System.out.println("Processing file " + csvFile);
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";

		int nextDateID = DB.getNextDateID();
		int nextSymbolID = DB.getNextSymbolID();
		PreparedStatement symbolStmnt = DB.getSymbolInsertStatement();
		PreparedStatement dateStmnt = DB.getDateInsertStatement();
		PreparedStatement rockStmnt = DB.getRockInsertStatement();
		PreparedStatement update520CXStmnt = DB.get520CXUpdateStmnt();

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
					if (recordExists && fileName.toLowerCase().indexOf("base") < 0) {
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
						// new code
						float percentage = 0.0f;
						try {
							String val = data[1].strip().substring(0, data[1].strip().length() - 1);
							// System.out.println("data[1] "+val);
							percentage = Float.parseFloat(val);
						} catch (Exception ex) {
							System.out.println("Percentage parse error...");
							ex.printStackTrace(System.out);
						}
						float close = 0.0f;
						try {
							close = Float.parseFloat(data[2].strip());
						} catch (Exception ex) {
							System.out.println("Close parse error...");
							ex.printStackTrace(System.out);
						}
						float netChange = 0.0f;
						try {
							netChange = Float.parseFloat(data[3].strip());
						} catch (Exception ex) {
							System.out.println("Netchange parse error...");
							ex.printStackTrace(System.out);
						}

						float open = 0.0f;
						try {
							open = Float.parseFloat(data[4].strip());
						} catch (Exception ex) {
							System.out.println("Open parse error...");
							ex.printStackTrace(System.out);
						}
						float high = 0.0f;
						try {
							high = Float.parseFloat(data[5].strip());
						} catch (Exception ex) {
							System.out.println("High parse error...");
							ex.printStackTrace(System.out);
						}
						float low = 0.0f;
						try {
							low = Float.parseFloat(data[6].strip());
						} catch (Exception ex) {
							System.out.println("Low parse error...");
							ex.printStackTrace(System.out);
						}
						float low52 = 0.0f;
						try {
							low52 = Float.parseFloat(data[7].strip());
						} catch (Exception ex) {
							System.out.println("Low52 parse error...");
							ex.printStackTrace(System.out);

						}
						float high52 = 0.0f;
						try {
							high52 = Float.parseFloat(data[8].strip());
						} catch (Exception ex) {
							System.out.println("High52 parse error...");
							ex.printStackTrace(System.out);

						}
						// 119.88,"53,714 M","1,003,250"
						float marketCap = 0.0f;
						try {
							// System.out.println(line);
							// System.out.println("Split 8 " + data[8]);
							// System.out.println("Split 9 " + data[9]);
							if (line.indexOf(" M") > 0) {
								String val = line.substring(line.indexOf(data[8]) + data[8].length() + 1,
										line.indexOf(" M"));
								while (val.indexOf(data[8]) >= 0) {
									//System.out.println(val);
									//System.out.println(data[8]);
									val = val.substring(val.indexOf(data[8]) + data[8].length() + 1, val.length());
								}
								//System.out.println(symbol + " data[9] " + val);
								// System.out.println(val.replaceAll(",", "").replaceAll("\"", ""));
								//if (val.indexOf("\"") >= 0) {
									marketCap = Float.parseFloat(val.replaceAll(",", "").replaceAll("\"", ""));
								//}else {
								//	marketCap = Float.parseFloat(val.substring(0,val.indexOf(",")));
								//}
							} else {
								marketCap = Float.parseFloat(data[9].replaceAll(",", ""));
							}
						} catch (Exception ex) {
							System.out.println(symbol + " : " + line);
							System.out.println("Marketcap parse error...");
							ex.printStackTrace(System.out);

						}
						// System.out.println("Split 10 " + data[10]);
						float volume = 0.0f;
						float atr = 0.0f;

						if (symbol.equalsIgnoreCase("BIO")) {
							System.out.println("Debug");
						}
						if (line.indexOf(" M\",\"") > 0) {
							int m1 = line.indexOf(" M\",\"");
							String s1 = line.substring(m1 + 5, line.length());
							// System.out.println(s1);
							String s2 = s1.substring(0, s1.indexOf("\""));
							try {
								volume = Float.parseFloat(s2.replace(",", ""));
							} catch (Exception ex) {
								System.out.println("Volume parse error...");
								ex.printStackTrace(System.out);

							}
							String s3 = s1.substring(s1.indexOf("\"") + 2, s1.length());
							try {
								if(!s3.strip().equalsIgnoreCase("NaN"))
								atr = Float.parseFloat(s3.replace(",", ""));
							} catch (Exception ex) {
								System.out.println("atr parse error...");
								ex.printStackTrace(System.out);

							}
						} else if (line.indexOf(" M\",") > 0) {
							int m1 = line.indexOf(" M\",");
							String s1 = line.substring(m1 + 4, line.length());
							// System.out.println(s1);
							String s2 = s1.substring(0, s1.indexOf(","));
							try {
								volume = Float.parseFloat(s2.replace(",", ""));
							} catch (Exception ex) {
								System.out.println("Volume parse error..."+symbol);
								ex.printStackTrace(System.out);

							}
							String s3 = s1.substring(s1.indexOf(",") + 1, s1.length());
							try {
								if(!s3.strip().equalsIgnoreCase("NaN"))
								atr = Float.parseFloat(s3.replace(",", ""));
							} catch (Exception ex) {
								System.out.println("atr parse error...");
								ex.printStackTrace(System.out);

							}
						} else if (line.indexOf(" M,\"") > 0) {
							int m1 = line.indexOf(" M,\"");
							String s1 = line.substring(m1 + 4, line.length());
							// System.out.println(s1);
							String s2 = s1.substring(0, s1.indexOf("\""));
							try {
								volume = Float.parseFloat(s2.replace(",", ""));
							} catch (Exception ex) {
								System.out.println("Volume parse error...");
								ex.printStackTrace(System.out);

							}
							String s3 = s1.substring(s1.indexOf("\"") + 2, s1.length());
							try {
								if(!s3.strip().equalsIgnoreCase("NaN"))
								atr = Float.parseFloat(s3.replace(",", ""));
							} catch (Exception ex) {
								System.out.println("atr parse error...");
								ex.printStackTrace(System.out);

							}
						} else if (line.indexOf(" M,") > 0) {
							int m1 = line.indexOf(" M,");
							String s1 = line.substring(m1 + 3, line.length());
							// System.out.println(s1);
							String s2 = s1.substring(0, s1.indexOf(","));
							try {
								volume = Float.parseFloat(s2.replace(",", ""));
							} catch (Exception ex) {
								System.out.println("Volume parse error..."+symbol);
								ex.printStackTrace(System.out);

							}
							String s3 = s1.substring(s1.indexOf(",") + 1, s1.length());
							try {
								if(!s3.strip().equalsIgnoreCase("NaN"))
								atr = Float.parseFloat(s3.replace(",", ""));
							} catch (Exception ex) {
								System.out.println("atr parse error..."+symbol);
								ex.printStackTrace(System.out);

							}
						} else if (line.indexOf(",\"") > 0) {
							int m1 = line.indexOf(",\"");
							String s1 = line.substring(m1 + 2, line.length());
							// System.out.println(s1);
							String s2 = s1.substring(0, s1.indexOf("\""));
							try {
								volume = Float.parseFloat(s2.replace(",", ""));
							} catch (Exception ex) {
								System.out.println("Volume parse error...");
								ex.printStackTrace(System.out);

							}
							String s3 = s1.substring(s1.indexOf("\",") + 2, s1.length());
							try {
								if(!s3.strip().equalsIgnoreCase("NaN"))
								atr = Float.parseFloat(s3.replace(",", ""));
							} catch (Exception ex) {
								System.out.println("atr parse error...");
								ex.printStackTrace(System.out);

							}
						}else {
							try {
								volume = Float.parseFloat(data[10]);
							} catch (Exception ex) {
								System.out.println("Volume parse error..."+symbol);
								ex.printStackTrace(System.out);

							}
							try {
								//System.out.println(line);
								//System.out.println(data[11]);
								if(!data[11].strip().equalsIgnoreCase("NaN"))
								atr = Float.parseFloat(data[11]);
							} catch (Exception ex) {
								System.out.println("atr parse error..."+symbol);
								ex.printStackTrace(System.out);

							}

						}

						// System.out.println("Split 11 " + data[11]);

						// if (symbol.equalsIgnoreCase("BRK/A"))
						// System.out.println(symbol + ": " + percentage + ": " + close + ": " +
						// netChange + ": " + open
						// + ": " + high + ": " + low + ": " + low52 + ": " + high52 + ": " + marketCap
						// + ": "
						// + volume + ": " + atr);
						// System.out.println("Country [code= " + country[4] + " , name=" + country[5] +
						// "]");
						// new code

						// insert records;
						if (!recordExists&&fileName.toLowerCase().indexOf("basecrx") < 0) {
							try {
						//		 System.out.println(symbol + ": " + percentage + ": " + close + ": " +
						//		 netChange + ": " + atr
						//		+ ": " + open + ": " + high + ": " + low + ": " + low52 + ": " + high52 
						//		+ ": " + marketCap + ": " + volume);
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
						} else if (fileName.toLowerCase().indexOf("basecrx") >= 0) {
							// update 520CX
							try {
								update520CXStmnt.setInt(1, (int) atr);
								update520CXStmnt.setInt(2, stockID);
								update520CXStmnt.setInt(3, currentDateID);
								update520CXStmnt.executeUpdate();
							} catch (Exception ex) {
								System.out.println("Update 520CX failed ");
								ex.printStackTrace(System.out);
							}

						}

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
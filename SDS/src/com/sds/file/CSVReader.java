package com.sds.file;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.io.*;
import java.util.*;

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
				ex.printStackTrace(System.out);
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
	/*
	 * "" ,"Order Date " ,"Order #" ,"Account" ,"Trade  Action" ,"Security"
	 * ,"Quantity" ,"Order Type" ,"Bid/Ask" ,"Last Price" ,"Duration" ,"Status" ""
	 * ,"1/29/2021 1:24 PM ET" ,"VVG-2315" ,"IRRA-Edge 24Z-16K80" ,"Buy to open"
	 * ," BIDU FEB 12, 2021 260.00 CALL" ,"1" ,"Limit$4.80" ,"$4.60 / $6.25"
	 * ,"$5.05" ,"Day " ,"Executed  1 @ $4.80" "" ,"1/29/2021 1:03 PM ET"
	 * ,"VVK-4733" ,"IRRA-Edge 24Z-16K80" ,"Buy to open"
	 * ," SOXL FEB 19, 2021 600.00 CALL" ,"1" ,"Limit$19.80" ,"$17.00 / $21.00"
	 * ,"$19.00" ,"Day " ,"Executed  1 @ $19.80" "" ,"1/28/2021 9:35 AM ET"
	 * ,"VVN-1587" ,"IRRA-Edge 24Z-16K80" ,"Buy to open"
	 * ," BIDU FEB 12, 2021 260.00 CALL" ,"5" ,"Limit$7.50" ,"$4.60 / $6.25"
	 * ,"$5.05" ,"Day " ,"Executed  5 @ $7.50"
	 */

	public static void parseMLExport(String folder) {
		// list all files under a folder
		File f = new File(folder);

		// Populates the array with names of files and directories
		String[] fnames = f.list();
		HashMap results = new HashMap();

		// For each pathname in the pathnames array
		for (String fname : fnames) {
			// Print the names of files and directories
			// System.out.println(fname);
			// loop parse files and extract the record
			readMELFile(folder, fname, results);

		}

		FileWriter myWriter = null;
		String path = "/home/joma/share/test/SMELL/";
		Calendar cal = Calendar.getInstance();
		int month = cal.get(Calendar.MONTH) + 1;
		int year = cal.get(Calendar.YEAR);
		int date = cal.get(Calendar.DAY_OF_MONTH);
		String fileName = "" + year + "_" + month + "_" + date + "_Sorted.txt";
		try {
			File myObj = new File(path + fileName);

			if (!myObj.exists())
				myObj.createNewFile();
			// if (myObj.createNewFile()) {
			Thread.sleep(5000);
			myWriter = new FileWriter(myObj);
			// myWriter.write(allStocksBuffer.toString());
			// myWriter.close();
			// System.out.println("File created: " + myObj.getName());
			// } else {
			// System.out.println("File already exists.");
			// }

			Map<String, TreeMap> sortedMap = new TreeMap<String, TreeMap>(results);

			Iterator stocks = sortedMap.keySet().iterator();
			String separator1 = "|";

			while (stocks.hasNext()) {
				String stock = stocks.next().toString();
				// TreeMap<Calendar, String> tree = new TreeMap<Calendar, String>();

				TreeMap<Calendar, String> records = (TreeMap<Calendar, String>) sortedMap.get(stock);

				Iterator recordsIT = records.keySet().iterator();
				while (recordsIT.hasNext()) {
					Calendar key = (Calendar) recordsIT.next();
					String line = records.get(key).toString();
					System.out.println(padtoLength(stock, 6) + separator1 + line);
					myWriter.write(padtoLength(stock, 4) + separator1 + line + "\n");

				}
				// myWriter.write("\n");
				System.out.println(" ");
			}

			myWriter.close();
		} catch (Exception e) {
			System.out.println("An error occurred.");
			e.printStackTrace();
		}
		// sort the records based on stock name

		// sort the records base on date time??
	}

	private static String padtoLength(String input, int length) {
		StringBuffer result = new StringBuffer(input);

		for (int k = 0; k < (length - input.length()); k++) {
			result.append(" ");
		}

		return result.toString();

	}

	public static void readMELFile(String path, String fileName, HashMap results) {
		String csvFile = path + fileName;
		// System.out.println("Processing file " + csvFile);
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = "\" ,\"";

		try {

			boolean start = false;
			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {

				if (line.indexOf("Buy") > 0 || line.indexOf("Sell") > 0) {
					start = true;
				}
				// use comma as separator
				if (start) {
					// "" ,"Order Date " ,"Order #" ,"Account" ,
					// "Trade Action" ,"Security" ,"Quantity" ,
					// "Order Type" ,"Bid/Ask" ,"Last Price" ,
					// "Duration" ,"Status"
					if (line.indexOf("Buy") < 0 && line.indexOf("Sell") < 0) {
						break;
					}

					// System.out.println(line);
					String[] data = line.split(cvsSplitBy);
					String emptySpace = data[0];
					String orderDate = data[1];
					String orderNum = data[2];
					String account = data[3];
					String action = data[4];
					String security = data[5];
					String quantity = data[6];
					String orderType = data[7];
					String bidAsk = data[8];
					String lastPrice = data[9];
					String duration = data[10];
					String status = data[11];
					String separator1 = "|";
					/*
					 * System.out.println(" orderDate " + orderDate);
					 * System.out.println(" orderNum " + orderNum); System.out.println(" account " +
					 * account); System.out.println(" action " + action);
					 * System.out.println(" security  " + security); System.out.println(" quantity "
					 * + quantity); System.out.println(" orderType " + orderType);
					 * System.out.println(" bidAsk " + bidAsk); System.out.println(" lastPrice  " +
					 * lastPrice); System.out.println(" duration " + duration);
					 * System.out.println(" status " + status);
					 */
					String[] sdata = security.strip().split(" ");
					String stock = sdata[0];
					// System.out.println(" stock " + stock);

					String[] dateInfo = orderDate.strip().split(" ");
					String dates = dateInfo[0];
					String hours = dateInfo[1];
					String map = dateInfo[2];
					String tzone = dateInfo[3];
					/*
					 * System.out.println("  dates  " + dates); System.out.println("  hours  " +
					 * hours); System.out.println("  map  " + map); System.out.println("  tzone  " +
					 * tzone);
					 */
					String[] may = dates.strip().split("/");
					int month = Integer.parseInt(may[0]);
					int day = Integer.parseInt(may[1]);
					int year = Integer.parseInt(may[2]);
					// System.out.println(" year " + year + " month " + month + " day " + day);

					String[] ham = hours.strip().split(":");
					int hour = Integer.parseInt(ham[0]);
					int minute = Integer.parseInt(ham[1]);
					// System.out.println(" hour " + hour + " minute " + minute + " AM/PM " + map);

					Calendar cal = Calendar.getInstance();
					cal.set(year, month - 1, day);
					cal.set(Calendar.HOUR, hour);
					cal.set(Calendar.MINUTE, minute);
					cal.set(Calendar.SECOND, 0);
					cal.set(Calendar.MILLISECOND, 0);
					if (map.equalsIgnoreCase("PM")) {
						cal.set(Calendar.AM_PM, 1);
					} else {
						cal.set(Calendar.AM_PM, 0);
					}

					String expirationDate = "";
					Calendar cal2 = cal;

					if (action.indexOf("Sell to open") >= 0 || (action.indexOf("Buy to open") >= 0)) {
						// this is option trade expiration date should be parsed out
						StringTokenizer toks = new StringTokenizer(security.trim(), " ,");
						String stk = toks.nextToken();
						String mStr = toks.nextToken();
						String dStr = toks.nextToken();
						String yStr = toks.nextToken();
						int m = 0;
						if (mStr.equalsIgnoreCase("JAN")) {
							m = 0;
						} else if (mStr.equalsIgnoreCase("FEB")) {
							m = 1;
						} else if (mStr.equalsIgnoreCase("MAR")) {
							m = 2;
						} else if (mStr.equalsIgnoreCase("AP")) {
							m = 3;
						} else if (mStr.equalsIgnoreCase("MAY")) {
							m = 4;
						} else if (mStr.equalsIgnoreCase("JUNE")) {
							m = 5;
						} else if (mStr.equalsIgnoreCase("JULY")) {
							m = 6;
						} else if (mStr.equalsIgnoreCase("AUG")) {
							m = 7;
						} else if (mStr.equalsIgnoreCase("SEPT")) {
							m = 8;
						} else if (mStr.equalsIgnoreCase("OCT")) {
							m = 9;
						} else if (mStr.equalsIgnoreCase("NOV")) {
							m = 10;
						} else if (mStr.equalsIgnoreCase("DEC")) {
							m = 11;
						}
						int y = Integer.parseInt(yStr);
						int d = Integer.parseInt(dStr);
						cal2 = Calendar.getInstance();
						cal2.set(y, m, d);
						cal2.set(Calendar.HOUR, 0);
						cal2.set(Calendar.MINUTE, 0);
						cal2.set(Calendar.SECOND, 0);
						cal2.set(Calendar.MILLISECOND, 0);
						expirationDate = "" + (cal2.get(Calendar.MONTH) + 1) + "/" + cal2.get(Calendar.DAY_OF_MONTH)
								+ "/" + cal2.get(Calendar.YEAR);

					} else if (action.indexOf("Buy") >= 0 && action.indexOf("Buy to open") < 0) {
						// this is stock, add one month to the initial buy date for tracking purpose
						cal2.add(Calendar.MONTH, 1);
						expirationDate = "" + (cal2.get(Calendar.MONTH) + 1) + "/" + cal2.get(Calendar.DAY_OF_MONTH)
								+ "/" + cal2.get(Calendar.YEAR);
					} else if ((action.indexOf("Sell") >= 0 && action.indexOf("Sell to open") < 0)
							|| action.indexOf("Sell to close") < 0) {
						// sell stock, expired the same day as sold
						expirationDate = "" + (cal2.get(Calendar.MONTH) + 1) + "/" + cal2.get(Calendar.DAY_OF_MONTH)
								+ "/" + cal2.get(Calendar.YEAR);
					}

					String type = "stock";
					if (action.indexOf("Sell to open") >= 0 || (action.indexOf("Buy to open") >= 0)
							|| (action.indexOf("Buy to close") >= 0) || (action.indexOf("Sell to close") >= 0)) {
						type = "options";
					}

					System.out.println(status);

					String[] sts = status.strip().split(" ");
					String sts1 = sts[0];
					//String vol = sts[1];
					String quant = sts[2];
					//String price = sts[3];
					String price = sts[4];
					if(price.indexOf("$")>=0)
					price = price.substring(0,price.length()-1);
					System.out.println("sts1 " + sts1);
					System.out.println("quantity " + quant);
					System.out.println("price " + price);

					String newline = padtoLength(orderDate, 24) + separator1 + padtoLength(action, 14) + separator1
							+ padtoLength(security, 32) + separator1 + padtoLength(quantity, 5) + separator1
							+ padtoLength(orderType, 15) + separator1 + padtoLength(status, 28) + separator1
							+ padtoLength(stock, 5) + separator1+ padtoLength(quant, 5)+separator1+ padtoLength(type, 8) +separator1 +padtoLength(price, 8) + separator1 + expirationDate;

					if (results.containsKey(stock)) {
						TreeMap<Calendar, String> records = (TreeMap<Calendar, String>) results.get(stock);
						records.put(cal, newline);

					} else {
						TreeMap<Calendar, String> records = new TreeMap<Calendar, String>();
						records.put(cal, newline);
						results.put(stock, records);
					}

				}
			}
		} catch (

		FileNotFoundException e) {
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

	public static void uploadCSVtoDB(String path, String fileName, String stock) {
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

					// symbol must not have small cases or /
					boolean validScopeSymbol = true;
					if (symbol.indexOf("/") >= 0 || symbol.compareTo(symbol.toUpperCase()) != 0)
						validScopeSymbol = false;

					if (validScopeSymbol && symbol.equalsIgnoreCase(stock)) {

						if (insertSymbol(symbol, nextSymbolID, symbolStmnt)) {
							nextSymbolID++;
						}
						int stockID = DB.getSymbolID(symbol);

						boolean recordExists = DB.checkBBRecordExist(stockID, currentDateID);
						if (recordExists && (fileName.toLowerCase().indexOf("base") < 0
								&& fileName.toLowerCase().indexOf("crx") < 0)) {
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
										// System.out.println(val);
										// System.out.println(data[8]);
										val = val.substring(val.indexOf(data[8]) + data[8].length() + 1, val.length());
									}
									// System.out.println(symbol + " data[9] " + val);
									// System.out.println(val.replaceAll(",", "").replaceAll("\"", ""));
									// if (val.indexOf("\"") >= 0) {
									marketCap = Float.parseFloat(val.replaceAll(",", "").replaceAll("\"", ""));
									// }else {
									// marketCap = Float.parseFloat(val.substring(0,val.indexOf(",")));
									// }
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
									if (!s3.strip().equalsIgnoreCase("NaN"))
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
									System.out.println("Volume parse error..." + symbol);
									ex.printStackTrace(System.out);

								}
								String s3 = s1.substring(s1.indexOf(",") + 1, s1.length());
								try {
									if (!s3.strip().equalsIgnoreCase("NaN"))
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
									if (!s3.strip().equalsIgnoreCase("NaN"))
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
									System.out.println("Volume parse error..." + symbol);
									ex.printStackTrace(System.out);

								}
								String s3 = s1.substring(s1.indexOf(",") + 1, s1.length());
								try {
									if (!s3.strip().equalsIgnoreCase("NaN"))
										atr = Float.parseFloat(s3.replace(",", ""));
								} catch (Exception ex) {
									System.out.println("atr parse error..." + symbol);
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
									if (!s3.strip().equalsIgnoreCase("NaN"))
										atr = Float.parseFloat(s3.replace(",", ""));
								} catch (Exception ex) {
									System.out.println("atr parse error...");
									ex.printStackTrace(System.out);

								}
							} else {
								try {
									volume = Float.parseFloat(data[10]);
								} catch (Exception ex) {
									System.out.println("Volume parse error..." + symbol);
									ex.printStackTrace(System.out);

								}
								try {
									// System.out.println(line);
									// System.out.println(data[11]);
									if (!data[11].strip().equalsIgnoreCase("NaN"))
										atr = Float.parseFloat(data[11]);
								} catch (Exception ex) {
									System.out.println("atr parse error..." + symbol);
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
							if (!recordExists && fileName.toLowerCase().indexOf("crx") < 0) {
								try {
									// System.out.println(symbol + ": " + percentage + ": " + close + ": " +
									// netChange + ": " + atr
									// + ": " + open + ": " + high + ": " + low + ": " + low52 + ": " + high52
									// + ": " + marketCap + ": " + volume);
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
							} else if (fileName.toLowerCase().indexOf("crx") >= 0) {
								// update 520CX
								try {
									if (((int) atr) < 1) {
										// basecrx file 0 means below thus -1
										update520CXStmnt.setInt(1, -1);
									} else {
										update520CXStmnt.setInt(1, (int) atr);
									}
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

					if (symbol.equalsIgnoreCase("ATNFW")) {
						System.out.println("Debug started...");
					}
					// symbol must not have small cases or /
					boolean validScopeSymbol = true;
					if (symbol.indexOf("/") >= 0 || symbol.compareTo(symbol.toUpperCase()) != 0)
						validScopeSymbol = false;

					if (validScopeSymbol && insertSymbol(symbol, nextSymbolID, symbolStmnt)) {
						nextSymbolID++;
					}
					int stockID = DB.getSymbolID(symbol);

					boolean recordExists = DB.checkBBRecordExist(stockID, currentDateID);
					if (validScopeSymbol && recordExists && (fileName.toLowerCase().indexOf("base") < 0
							|| fileName.toLowerCase().indexOf("crx") < 0)) {
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

					} else if (validScopeSymbol) {
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
									// System.out.println(val);
									// System.out.println(data[8]);
									val = val.substring(val.indexOf(data[8]) + data[8].length() + 1, val.length());
								}
								// System.out.println(symbol + " data[9] " + val);
								// System.out.println(val.replaceAll(",", "").replaceAll("\"", ""));
								// if (val.indexOf("\"") >= 0) {
								marketCap = Float.parseFloat(val.replaceAll(",", "").replaceAll("\"", ""));
								// }else {
								// marketCap = Float.parseFloat(val.substring(0,val.indexOf(",")));
								// }
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
								if (!s3.strip().equalsIgnoreCase("NaN"))
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
								System.out.println("Volume parse error..." + symbol);
								ex.printStackTrace(System.out);

							}
							String s3 = s1.substring(s1.indexOf(",") + 1, s1.length());
							try {
								if (!s3.strip().equalsIgnoreCase("NaN")) {
									atr = Float.parseFloat(s3.replace(",", ""));
								} else {
									atr = -1000.0f;
								}
							} catch (Exception ex) {
								atr = -8000.0f;
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
								if (!s3.strip().equalsIgnoreCase("NaN"))
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
								System.out.println("Volume parse error..." + symbol);
								ex.printStackTrace(System.out);

							}
							String s3 = s1.substring(s1.indexOf(",") + 1, s1.length());
							try {
								if (!s3.strip().equalsIgnoreCase("NaN")) {
									atr = Float.parseFloat(s3.replace(",", ""));
								} else {
									atr = -1000.0f;
								}
							} catch (Exception ex) {
								atr = -8000.0f;
								System.out.println("atr parse error..." + symbol);
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
								if (!s3.strip().equalsIgnoreCase("NaN")) {
									atr = Float.parseFloat(s3.replace(",", ""));
								} else {
									atr = -1000.0f;
								}
							} catch (Exception ex) {
								atr = -8000.0f;
								System.out.println("atr parse error...");
								ex.printStackTrace(System.out);

							}
						} else {
							try {
								volume = Float.parseFloat(data[10]);
							} catch (Exception ex) {
								System.out.println("Volume parse error..." + symbol);
								ex.printStackTrace(System.out);

							}
							try {
								// System.out.println(line);
								// System.out.println(data[11]);
								if (!data[11].strip().equalsIgnoreCase("NaN"))
									atr = Float.parseFloat(data[11]);
							} catch (Exception ex) {
								System.out.println("atr parse error..." + symbol);
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
						if (validScopeSymbol && !recordExists && fileName.toLowerCase().indexOf("crx") < 0) {
							try {
								// System.out.println(symbol + ": " + percentage + ": " + close + ": " +
								// netChange + ": " + atr
								// + ": " + open + ": " + high + ": " + low + ": " + low52 + ": " + high52
								// + ": " + marketCap + ": " + volume);
								rockStmnt.setInt(1, stockID);
								rockStmnt.setInt(2, currentDateID);
								rockStmnt.setFloat(3, percentage);
								rockStmnt.setFloat(4, close);
								rockStmnt.setFloat(5, netChange);
								if (atr > -999.0f && atr < -7999.0f) {
									atr = 0.0f;
								}
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
								System.out.println("File " + fileName);
								System.out.println(stockID + " stockID " + symbol);
								ex.printStackTrace(System.out);
							}
							// insert records;
						} else if (validScopeSymbol && fileName.toLowerCase().indexOf("crx") >= 0) {
							// update 520CX
							try {
								if (((int) atr) < -999 && ((int) atr) > -1001) {
									// basecrx file 0 means below thus -1
									update520CXStmnt.setInt(1, -10);
								} else if (((int) atr) < -7999 && ((int) atr) > -8001) {
									// basecrx file 0 means below thus -1
									update520CXStmnt.setInt(1, -20);
								} else if (((int) atr) < 1) {
									// basecrx file 0 means below thus -1
									update520CXStmnt.setInt(1, -1);
								} else {
									update520CXStmnt.setInt(1, (int) atr);
								}
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

	public static void uploadIndustryCSVtoDB(String path, String fileName) {
		String csvFile = path + fileName;
		System.out.println("Processing file " + csvFile);
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";

		// String query = "select INDID , INDUSTRY FROM INDUSTRY ORDER BY INDUSTRY ASC";
		PreparedStatement industryStmnt = DB.getIndustryStmnt();
		PreparedStatement insertIndustryStmnt = DB.getIndInsertStatement();
		PreparedStatement subIndustryStmnt = DB.getSubIndustryStmnt();
		PreparedStatement insertSubIndustryStmnt = DB.getSubIndInsertStatement();
		PreparedStatement subUnderIndStmnt = DB.getAllSubUnderIndustryStmnt();
		PreparedStatement updateStockIndustryCode = DB.updateStockIndustryCode();
		PreparedStatement IndCodeStmnt = DB.getIndustryCodeStmnt();

		try {

			ResultSet rs1 = industryStmnt.executeQuery();

			Hashtable industries = new Hashtable();

			while (rs1.next()) {
				int iid = rs1.getInt(1);
				String ind = rs1.getString(2);
				industries.put(ind, "" + iid);
			}

			int indSize = industries.size();

			// String query = "select a.INDID , SUBID,INDUSTRY, SUBINDUSTRY FROM INDUSTRY a,
			// SUBINDUSTRY b where a.INDID=b.INDID ORDER BY a.INDUSTRY ASC, b.SUBINDUSTRY
			// ASC;";

			boolean start = false;
			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {

				String industry = "";
				String subIndustry = "";
				String symbol = "";
				if (start) {

					if (line.indexOf("\"") < 0) {// no " quick split
						// use comma as separator
						String[] data = line.split(cvsSplitBy);
						symbol = data[0];
						try {
							industry = data[1];
						} catch (Exception e) {

						}
						try {
							subIndustry = data[2];
						} catch (Exception e) {

						}
					} else { // if there is quote, loop through character by character
						int length = line.length();
						boolean quoteStart = false;
						boolean quoteEnd = false;
						boolean commaFound = false;
						StringBuffer buff = new StringBuffer();
						for (int k = 0; k < length; k++) {
							if (line.charAt(k) == '"' && !quoteStart) {
								quoteStart = true;
							} else if (line.charAt(k) == '"' && quoteStart) {
								quoteEnd = true;
								if (industry.length() == 0) {
									industry = buff.toString();
									buff = new StringBuffer();
									quoteStart = false;
									quoteEnd = false;
								} else {
									subIndustry = buff.toString();
								}
							} else if (line.charAt(k) == ',' && quoteStart) {
								// , in the middle of quotation, append
								buff.append(line.charAt(k));
							} else if (line.charAt(k) == ',' && !quoteStart) {
								// ignore it, as it is the proper separator, except the first
								// , tells you the previous value is symbol
								if (symbol.length() == 0) {
									symbol = buff.toString();
									buff = new StringBuffer();
								}
							} else {
								buff.append(line.charAt(k));
							}

						}

						if (subIndustry.length() == 0) {
							subIndustry = buff.toString();
						}
					}

					if (industry != null && industry.length() > 2) {
						// insert industry
						// industry = "Oil: Gas & Consumable Fuels";
						System.out.println(industry);
						industry = industry.replace("\"", "");
						industry = industry.replace(":", "");
						if (!industries.containsKey(industry)) {
							insertIndustryStmnt.setInt(1, ++indSize);
							insertIndustryStmnt.setString(2, industry);
							System.out.println("Try to insert " + industry + " :" + indSize);
							insertIndustryStmnt.execute();
							industries.put(industry, indSize);
						}

						if (subIndustry != null && subIndustry.length() > 2) {
							// insert subIndustry
							// String query = "insert into SUBINDUSTRY (INDID ,SUBID, SUBINDUSTRY) values
							// (?, ?, ?)";
							if (industries.containsKey(industry)) {
								int indid = Integer.parseInt(industries.get(industry).toString());
								subUnderIndStmnt.setInt(1, indid);
								ResultSet rs2 = subUnderIndStmnt.executeQuery();
								Hashtable subIndMap = new Hashtable();
								// String query = "select INDID , SUBID,SUBINDUSTRY FROM SUBINDUSTRY where INDID
								// = ?";

								while (rs2.next()) {
									String subInd = rs2.getString(3);
									int subid = rs2.getInt(2);
									subIndMap.put(subInd, "" + subid);
								}

								// String query = "insert into SUBINDUSTRY (INDID ,SUBID, SUBINDUSTRY) values
								// (?, ?, ?)";
								if (!subIndMap.containsKey(subIndustry)) {
									insertSubIndustryStmnt.setInt(1, indid);
									insertSubIndustryStmnt.setInt(2, subIndMap.size() + 1);
									insertSubIndustryStmnt.setString(3, subIndustry);
									insertSubIndustryStmnt.execute();
								}
							}

							// String query = "select a.INDID , SUBID,INDUSTRY, SUBINDUSTRY
							// FROM INDUSTRY a, SUBINDUSTRY b where a.INDID=b.INDID AND a.INDUSTRY=?,
							// b.SUBINDUSTRY=?";

							IndCodeStmnt.setString(1, industry);
							IndCodeStmnt.setString(2, subIndustry);
							ResultSet rs4 = IndCodeStmnt.executeQuery();
							if (rs4.next()) {
								int indid = rs4.getInt(1);
								int subid = rs4.getInt(2);

								// String query = "UPDATE SYMBOLS SET INDID = ?, SUBID = ? WHERE SYMBOL = ?";
								updateStockIndustryCode.setInt(1, indid);
								updateStockIndustryCode.setInt(2, subid);
								updateStockIndustryCode.setString(3, symbol);
								updateStockIndustryCode.execute();
								System.out.println(symbol + " -- " + industry + ": " + subIndustry);
							}
						}

					}

				}
				if (line.indexOf("Sub-Industry") >= 0 && line.indexOf("Industry") >= 0)
					start = true;

			}

		} catch (Exception ex) {
			ex.printStackTrace(System.out);
		}
	}
}
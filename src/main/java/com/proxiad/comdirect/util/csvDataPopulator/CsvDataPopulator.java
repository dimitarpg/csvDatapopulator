package com.proxiad.comdirect.util.csvDataPopulator;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

public class CsvDataPopulator {
	private static Properties appProperties = new Properties();
	private static final String PARAM_CLEAR = "--clear";
	private static final String PARAM_TABLE = "-t";
	private static final String PARAM_USER = "-u";
	private static final String PARAM_PASS = "-p";
	private static final String PARAM_ADDRESS = "-a";

	public static void main(String[] args) {
		logInfo("========================================================================");
		logInfo("start CVS DATA POPULATOR");
		try {
			appProperties = new Properties();
			appProperties.load(CsvDataPopulator.class.getResourceAsStream("app.properties"));
			Map<String, String> cmdArguments = checkCommandLineArguments(args);
			logInfo("start csv processign--");
			Map<String, List<String>> parcedCSVdoc = processCSVFile(cmdArguments);
			logInfo("finish csv processign--");
			logInfo("start db population--");
			populateDB(cmdArguments, parcedCSVdoc);
			logInfo("finish db population--");
			logInfo("===SUCCESS==");
		} catch (Exception e) {
			logInfo("===FAIL==");
			e.printStackTrace();
		} finally {
			logInfo("finish CVS DATA POPULATOR ");
			logInfo("========================================================================");
		}
	}

	private static Map<String, List<String>> processCSVFile(Map<String, String> cmdArguments) throws Exception {
		Scanner csvReader = null;
		String csvLine = null;
		List<String> csvRowCells = null;
		StringBuilder csvFileBuilder = new StringBuilder();
		Map<String, List<String>> parcedCSVdoc = null;
		try {
			csvReader = new Scanner(new File(cmdArguments.get("-f")));
			parcedCSVdoc = new HashMap<String, List<String>>();
			// the column line in the file should be well formated
			if (csvReader.hasNext()) {
				csvLine = csvReader.nextLine();
				csvRowCells = Arrays.asList(csvLine.split(appProperties.getProperty("csvCellDelimiterRegex")));
				if (csvRowCells.size() > 0) {
					// removes the quotes from the first and the last element
					removeSymbolsFromACell(csvRowCells, 0, 1, csvRowCells.get(0).length());
					removeSymbolsFromACell(csvRowCells, csvRowCells.size() - 1, 0,
							csvRowCells.get(csvRowCells.size() - 1).length() - 1);
				}
				parcedCSVdoc.put("columnCells", csvRowCells);
				logInfo("columns(" + csvRowCells.size() + ") read:" + csvRowCells);
			}
			int csvDataRow = 0;
			while (csvReader.hasNext()) {
				csvLine = csvReader.nextLine();
				csvFileBuilder.append(csvLine);
				if (csvLine.charAt(csvLine.length() - 1) == '"') {
					// A Valid end of the line
					csvFileBuilder.append(";");
				}
				// logInfo("csv(" + csvDataRow + ") row read:" + csvLine);
				csvDataRow++;
				if (csvDataRow % 100 == 0 && csvDataRow > 0) {
					logInfo(csvDataRow + " lines read");
				}
			}
			logInfo("Total " + csvDataRow + " lines read");
			csvRowCells = Arrays
					.asList(csvFileBuilder.toString().split(appProperties.getProperty("csvCellDelimiterRegex")));
			if (csvRowCells.size() > 0) {
				// removes the quotes from the first and the last element
				removeSymbolsFromACell(csvRowCells, 0, 1, csvRowCells.get(0).length());
				removeSymbolsFromACell(csvRowCells, csvRowCells.size() - 1, 0,
						csvRowCells.get(csvRowCells.size() - 1).length() - 2);
			}
			// logInfo("csv data read:" + csvRowCells);
			parcedCSVdoc.put("dataCells", csvRowCells);
		} finally {
			if (csvReader != null) {
				csvReader.close();
			}
		}
		return parcedCSVdoc;
	}

	private static void populateDB(Map<String, String> arguments, Map<String, List<String>> parcedCSVdoc)
			throws Exception {
		String stmt = null;
		Connection connection = null;
		PreparedStatement insertSt = null;
		try {
			JdbcConnection jdbcConnection = JdbcConnection.getInstance(arguments.get(PARAM_ADDRESS),
					arguments.get(PARAM_USER), arguments.get(PARAM_PASS));
			connection = jdbcConnection.getConnection();
			if (arguments.get(PARAM_CLEAR) != null) {
				stmt = "DELETE FROM " + arguments.get(PARAM_TABLE);
				insertSt = connection.prepareStatement(stmt);
				insertSt.executeUpdate();
			}
			connection.setAutoCommit(false);

			String columnCells = formatCSVData(parcedCSVdoc.get("columnCells"), false);

			int numberOfColumns = parcedCSVdoc.get("columnCells").size();
			List<String> dataCells = parcedCSVdoc.get("dataCells");
			List<String> insertStatementValues;

			for (int i = 0; i < dataCells.size(); i = i + numberOfColumns) {
				insertStatementValues = dataCells.subList(i, i + numberOfColumns);
				String insertValues = formatCSVData(insertStatementValues, true);
				stmt = (String) appProperties.get("insertStatement");
				stmt = stmt.replaceAll("\\?", "%s");
				stmt = String.format(stmt, arguments.get(PARAM_TABLE), columnCells, insertValues);
				insertSt = connection.prepareStatement(stmt);
				// logInfo("execute insert statemet{" + i + "}: " + insertSt.toString());
				insertSt.executeUpdate();
			}
			connection.commit();
			JdbcConnection.closeConnection(connection, insertSt);

		} catch (Exception e) {
			throw new Exception("Statement: '" + stmt + "' caused by:", e);
		} finally {
			JdbcConnection.closeConnection(connection, insertSt);
		}
	}

	private static void logInfo(Object string) {
		System.out.println(string);
	}

	private static String formatCSVData(List<String> csvCells, boolean surroundWithSingleQuotes) {
		if (csvCells != null && csvCells.size() > 0) {
			SimpleDateFormat cvsDateFormat = new SimpleDateFormat(appProperties.getProperty("csvDateFormatIn"));
			StringBuilder valuesBuilder = new StringBuilder();
			for (int i = 0; i < csvCells.size(); i++) {
				String cellValue = csvCells.get(i);
				try {
					cvsDateFormat.parse(cellValue.replaceAll("\"", ""));
					valuesBuilder.append(" TO_DATE('" + cellValue.replaceAll("\"", "") + "', '"
							+ appProperties.getProperty("csvDateFormatOut") + "') ");
				} catch (ParseException e) {
					if (surroundWithSingleQuotes) {
						valuesBuilder.append("'" + cellValue + "'");
					} else {
						valuesBuilder.append(cellValue);
					}
				}
				if (i != csvCells.size() - 1) {
					valuesBuilder.append(",");
				}
			}
			return valuesBuilder.toString();
		} else {
			return null;
		}
	}

	private static Map<String, String> checkCommandLineArguments(String[] cmdArguments) throws Exception {
		Map<String, String> argMap = new HashMap<String, String>();
		if (cmdArguments.length == 0) {
			throw new Exception("No cmd argument provided!");
		} else {
			ArrayList<String> cmdArgumentsList = new ArrayList<String>(Arrays.asList(cmdArguments));
			try {
				String key;
				String value;
				if (cmdArgumentsList.contains(PARAM_USER)) {
					key = cmdArgumentsList.get(cmdArgumentsList.indexOf(PARAM_USER));
					value = cmdArgumentsList.get(cmdArgumentsList.indexOf(PARAM_USER) + 1);
					argMap.put(key, value);
				} else {
					throw new Exception("Missing argument : -u");
				}
				if (cmdArgumentsList.contains(PARAM_PASS)) {
					key = cmdArgumentsList.get(cmdArgumentsList.indexOf(PARAM_PASS));
					value = cmdArgumentsList.get(cmdArgumentsList.indexOf(PARAM_PASS) + 1);
					argMap.put(key, value);
				} else {
					throw new Exception("Missing argument : -p");
				}
				if (cmdArgumentsList.contains(PARAM_ADDRESS)) {
					// expected in format 'myhost:1521:orcl'
					key = cmdArgumentsList.get(cmdArgumentsList.indexOf(PARAM_ADDRESS));
					value = cmdArgumentsList.get(cmdArgumentsList.indexOf(PARAM_ADDRESS) + 1);
					argMap.put(key, value);
				} else {
					throw new Exception("Missing argument :-a");
				}
				if (cmdArgumentsList.contains(PARAM_TABLE)) {
					key = cmdArgumentsList.get(cmdArgumentsList.indexOf(PARAM_TABLE));
					value = cmdArgumentsList.get(cmdArgumentsList.indexOf(PARAM_TABLE) + 1);
					argMap.put(key, value);
				} else {
					throw new Exception("Missing argument : -t");
				}
				if (cmdArgumentsList.contains("-f")) {
					key = cmdArgumentsList.get(cmdArgumentsList.indexOf("-f"));
					value = cmdArgumentsList.get(cmdArgumentsList.indexOf("-f") + 1);
					argMap.put(key, value);
				} else {
					throw new Exception("Missing argument : -f");
				}
				if (cmdArgumentsList.contains(PARAM_CLEAR)) {
					argMap.put(PARAM_CLEAR, "YES!");
				}
			} catch (IndexOutOfBoundsException ex) {
				throw new Exception("Arguments missmatch!");
			}
		}
		return argMap;
	}

	private static void removeSymbolsFromACell(List<String> cells, int position, int startIndex, int endIndex) {
		if (cells.size() > 0) {
			String csvCell = cells.get(position);
			String formatedCsvCell = csvCell.substring(startIndex, endIndex);
			cells.set(position, formatedCsvCell);
		}
	}
}

package com.proxiad.comdirect.util.csvDataPopulator;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

import org.apache.log4j.Logger;

public class CsvDataPopulator {

	private static Logger log = Logger.getLogger(CsvDataPopulator.class);
	private static Properties appProperties = new Properties();

	public static void main(String[] args) {
		log.info("========================================================================");
		log.info("start CVS DATA POPULATOR");
		try {
			appProperties = new Properties();
			appProperties.load(CsvDataPopulator.class.getResourceAsStream("app.properties"));
			Map<String, String> cmdArguments = checkCommandLineArguments(args);

			log.info("start csv processign--");
			Map<String, List<String>> parcedCSVdoc = processCSVFile(cmdArguments);
			log.info("finish csv processign--");

			log.info("start db population--");
			populateDB(cmdArguments, parcedCSVdoc);
			log.info("finish db population--");

			log.info("===SUCCESS==");
		} catch (Exception e) {
			log.info("===FAIL==");
			e.printStackTrace();
		} finally {
			log.info("finish CVS DATA POPULATOR ");
			log.info("========================================================================");
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
				log.info("columns(" + csvRowCells.size() + ") read:" + csvRowCells);
			}

			int csvDataRow = 0;
			while (csvReader.hasNext()) {
				csvLine = csvReader.nextLine();
				csvFileBuilder.append(csvLine);
				if (csvLine.charAt(csvLine.length() - 1) == '"') {
					// A Valid end of the line
					csvFileBuilder.append(";");
				}
				log.info("csv(" + csvDataRow + ") row read:" + csvLine);
				csvDataRow++;
			}

			csvRowCells = Arrays
					.asList(csvFileBuilder.toString().split(appProperties.getProperty("csvCellDelimiterRegex")));
			if (csvRowCells.size() > 0) {
				// removes the quotes from the first and the last element
				removeSymbolsFromACell(csvRowCells, 0, 1, csvRowCells.get(0).length());
				removeSymbolsFromACell(csvRowCells, csvRowCells.size() - 1, 0,
						csvRowCells.get(csvRowCells.size() - 1).length() - 2);
			}
			log.info("csv data read:" + csvRowCells);

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
		JdbcConnection jdbcConnection = JdbcConnection.getInstance(arguments.get("-a"), arguments.get("-u"),
				arguments.get("-p"));
		Connection connection = jdbcConnection.getConnection();

		if (connection != null) {
			connection.setAutoCommit(false);
			PreparedStatement insertSt = null;

			String columnCells = formatCSVData(parcedCSVdoc.get("columnCells"), false);
			int numberOfColumns = parcedCSVdoc.get("columnCells").size();

			List<String> dataCells = parcedCSVdoc.get("dataCells");
			List<String> insertStatementValues;
			for (int i = 0; i < dataCells.size(); i = i + numberOfColumns) {
				insertStatementValues = dataCells.subList(i, i + numberOfColumns);
				String insertValues = formatCSVData(insertStatementValues, true);

				System.out.println("============= insertValues : " + insertValues);

				insertSt = connection.prepareStatement((String) appProperties.get("insertStatement"));
				insertSt.setString(0, arguments.get("-t"));
				insertSt.setString(1, columnCells);
				insertSt.setString(2, insertValues);

				log.info("execute insert statemet{" + i + "}: " + insertSt.toString());
				insertSt.executeUpdate();
			}

			connection.commit();
			JdbcConnection.closeConnection(connection, (Statement) insertSt);
		} else {
			throw new Exception("DB Connection Issue !");
		}
	}

	private static String formatCSVData(List<String> csvCells, boolean surroundWithSingleQuotes) {
		if (csvCells != null && csvCells.size() > 0) {
			SimpleDateFormat cvsDateFormat = new SimpleDateFormat(appProperties.getProperty("csvDateFormat"));
			StringBuilder valuesBuilder = new StringBuilder();

			for (int i = 0; i < csvCells.size(); i++) {
				String cellValue = csvCells.get(i);
				try {
					cvsDateFormat.parse(cellValue.replaceAll("\"", ""));
					valuesBuilder.append(" TO_DATE('" + cellValue.replaceAll("\"", "") + "', '"
							+ appProperties.getProperty("csvDateFormat") + "') ");
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
		} else
			return null;
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

				if (cmdArgumentsList.contains("-u")) {
					key = cmdArgumentsList.get(cmdArgumentsList.indexOf("-u"));
					value = cmdArgumentsList.get(cmdArgumentsList.indexOf("-u") + 1);
					argMap.put(key, value);
				} else {
					throw new Exception("Missing argument : -u");
				}

				if (cmdArgumentsList.contains("-p")) {
					key = cmdArgumentsList.get(cmdArgumentsList.indexOf("-p"));
					value = cmdArgumentsList.get(cmdArgumentsList.indexOf("-p") + 1);
					argMap.put(key, value);
				} else {
					throw new Exception("Missing argument : -p");
				}

				if (cmdArgumentsList.contains("-a")) {
					// expected in format 'myhost:1521:orcl'
					key = cmdArgumentsList.get(cmdArgumentsList.indexOf("-a"));
					value = cmdArgumentsList.get(cmdArgumentsList.indexOf("-a") + 1);
					argMap.put(key, value);
				} else {
					throw new Exception("Missing argument :-a");
				}

				if (cmdArgumentsList.contains("-t")) {
					key = cmdArgumentsList.get(cmdArgumentsList.indexOf("-t"));
					value = cmdArgumentsList.get(cmdArgumentsList.indexOf("-t") + 1);
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

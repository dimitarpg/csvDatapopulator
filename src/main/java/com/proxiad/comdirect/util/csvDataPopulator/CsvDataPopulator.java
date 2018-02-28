package com.proxiad.comdirect.util.csvDataPopulator;

import java.io.FileReader;
import java.io.IOException;
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
import org.apache.log4j.Logger;
import com.opencsv.CSVReader;

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

			log.info("==SUCCESS==");
		} catch (Exception e) {
			log.info("==FAIL==");
			e.printStackTrace();
		} finally {
			log.info("finish CVS DATA POPULATOR ");
			log.info("========================================================================");
		}
	}

	private static Map<String, List<String>> processCSVFile(Map<String, String> cmdArguments) throws Exception {
		CSVReader csvReader = null;
		Map<String, List<String>> parcedCSVdoc = null;
		try {
			csvReader = new CSVReader(new FileReader(cmdArguments.get("-f")), ',', '\'', 0);
			parcedCSVdoc = new HashMap<String, List<String>>();

			String[] line;
			String[] columnNames;
			if ((columnNames = csvReader.readNext()) != null) {
				log.info("columns: " + Arrays.asList(columnNames));
				parcedCSVdoc.put("columnNames: ", Arrays.asList(columnNames));
			}

			int dbRow = 0;
			while ((line = csvReader.readNext()) != null) {
				parcedCSVdoc.put(String.valueOf(dbRow), Arrays.asList(line));
				log.info("row value: " + dbRow + " == :" + Arrays.asList(line));
				dbRow++;
			}
		} finally {
			try {
				if (csvReader != null) {
					csvReader.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return parcedCSVdoc;
	}

	private static void populateDB(Map<String, String> arguments, Map<String, List<String>> parcedCSVdoc)
			throws Exception {
		com.proxiad.comdirect.util.csvDataPopulator.JdbcConnection jdbcConnection = JdbcConnection
				.getInstance(arguments.get("-a"), arguments.get("-u"), arguments.get("-p"));
		Connection connection = jdbcConnection.getConnection();
		if (connection != null) {
			connection.setAutoCommit(false);
			PreparedStatement insertSt = null;

			String columnNames = formatCSVData(parcedCSVdoc.get("columnNames"));

			for (int i = 0; i < parcedCSVdoc.values().size(); i++) {
				String insertValues;
				List<String> cellValues = parcedCSVdoc.get(String.valueOf(i));
				if (cellValues != null) {
					insertValues = formatCSVData(parcedCSVdoc.get(String.valueOf(i)));
				} else {
					break;
				}

				insertSt = connection.prepareStatement((String) appProperties.get("insertStatement"));
				insertSt.setString(0, arguments.get("-t"));
				insertSt.setString(1, columnNames);
				insertSt.setString(2, insertValues);

				log.info("==================== Execute insert statemet {" + i + "}:" + insertSt.toString());
				insertSt.executeUpdate();
			}

			connection.commit();
			JdbcConnection.closeConnection(connection, (Statement) insertSt);
		} else {
			throw new Exception("DB Connection Issue !");
		}
	}

	private static String formatCSVData(List<String> csvCells) {

		SimpleDateFormat cvsDateFormat = new SimpleDateFormat(appProperties.getProperty("csvDateFormat"));
		StringBuilder valuesBuilder = new StringBuilder();

		for (int i = 0; i < csvCells.size(); i++) {
			// TODO should i do that at all i think that open csv is doing that by defautl
			// builder.append(csvCells.get(i).replaceAll("\"", "\'"));

			// for the date values - (TO_DATE('2003/05/03 21:02:44', 'yyyy/mm/dd
			// hh24:mi:ss'));
			// my input 21.10.2014 07:47:31

			String cellValue = csvCells.get(i);
			try {
				cvsDateFormat.parse(cellValue.replaceAll("\"", ""));
				valuesBuilder.append(" (TO_DATE('" + cellValue.replaceAll("\"", "") + "', '"
						+ appProperties.getProperty("csvDateFormat") + "') ");
			} catch (ParseException e) {
				valuesBuilder.append(cellValue.replaceAll("\"", "\'"));
			}

			if (i != csvCells.size() - 1) {
				valuesBuilder.append(",");
			}
		}
		return valuesBuilder.toString();
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
}

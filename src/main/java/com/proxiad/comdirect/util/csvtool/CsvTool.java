package com.proxiad.comdirect.util.csvtool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class CsvTool extends SimpleAppLogger {
	private static Properties appProperties = new Properties();

	private static final String PARAM_CLEAR = "--clear";
	private static final String PARAM_EXPORT_ACTION = "--export";
	private static final String R_PARAM_TABLE = "-t";
	private static final String R_PARAM_USER = "-u";
	private static final String R_PARAM_PASS = "-p";
	private static final String R_PARAM_ADDRESS = "-a";
	private static final String R_PARAM_FILE = "-f";

	public static void main(String[] args) {
		logInfo("========================================================================");
		logInfo("CVS Tool START");
		try {
			Map<String, String> cmdArguments = checkCommandLineArguments(args);

			appProperties = new Properties();
			appProperties.load(CsvTool.class.getClassLoader().getResourceAsStream("app.properties"));
			ComdirectDAO cDao = new ComdirectDAO(appProperties);
			CsvFileUtils csvUtils = new CsvFileUtils(appProperties);

			if (cmdArguments.get(PARAM_EXPORT_ACTION) != null) {
				logInfo("[EXPORT DATA]");

				logInfo("start db extraction--");
				Map<String, List<String>> dbData = cDao.readDbData(cmdArguments);
				logInfo("finish db extraction--");

				logInfo("start db population--");
				String csvFileName = cmdArguments.get(R_PARAM_FILE);
				csvUtils.writeCsvFile(cmdArguments, dbData, csvFileName);
				logInfo("finish db population--");

			} else {
				logInfo("[IMPORT DATA]");

				logInfo("start csv processign--");
				Map<String, List<String>> parcedCSVdoc = csvUtils.readCsvFile(cmdArguments);
				logInfo("finish csv processign--");

				logInfo("start db population--");
				cDao.writeDbData(cmdArguments, parcedCSVdoc);
				logInfo("finish db population--");
			}
			logInfo("===SUCCESS==");
		} catch (Exception e) {
			logInfo("===FAIL==");
			e.printStackTrace();
		} finally {
			logInfo("CVS Tool FINISH");
			logInfo("========================================================================");
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
				if (cmdArgumentsList.contains(R_PARAM_USER)) {
					key = cmdArgumentsList.get(cmdArgumentsList.indexOf(R_PARAM_USER));
					value = cmdArgumentsList.get(cmdArgumentsList.indexOf(R_PARAM_USER) + 1);
					argMap.put(key, value);
				} else {
					throw new Exception("Missing argument: -u");
				}
				if (cmdArgumentsList.contains(R_PARAM_PASS)) {
					key = cmdArgumentsList.get(cmdArgumentsList.indexOf(R_PARAM_PASS));
					value = cmdArgumentsList.get(cmdArgumentsList.indexOf(R_PARAM_PASS) + 1);
					argMap.put(key, value);
				} else {
					throw new Exception("Missing argument: -p");
				}
				if (cmdArgumentsList.contains(R_PARAM_ADDRESS)) {
					// expected in format 'myhost:1521:orcl'
					key = cmdArgumentsList.get(cmdArgumentsList.indexOf(R_PARAM_ADDRESS));
					value = cmdArgumentsList.get(cmdArgumentsList.indexOf(R_PARAM_ADDRESS) + 1);
					argMap.put(key, value);
				} else {
					throw new Exception("Missing argument:-a");
				}
				if (cmdArgumentsList.contains(R_PARAM_TABLE)) {
					key = cmdArgumentsList.get(cmdArgumentsList.indexOf(R_PARAM_TABLE));
					value = cmdArgumentsList.get(cmdArgumentsList.indexOf(R_PARAM_TABLE) + 1);
					argMap.put(key, value);
				} else {
					throw new Exception("Missing argument: -t");
				}
				if (cmdArgumentsList.contains(R_PARAM_FILE)) {
					key = cmdArgumentsList.get(cmdArgumentsList.indexOf(R_PARAM_FILE));
					value = cmdArgumentsList.get(cmdArgumentsList.indexOf(R_PARAM_FILE) + 1);
					argMap.put(key, value);
				} else {
					throw new Exception("Missing argument: -f");
				}

				// not required arguments
				if (cmdArgumentsList.contains(PARAM_CLEAR)) {
					argMap.put(PARAM_CLEAR, "YES!");
				}
				if (cmdArgumentsList.contains(PARAM_EXPORT_ACTION)) {
					argMap.put(PARAM_EXPORT_ACTION, "YES!");
				}

			} catch (IndexOutOfBoundsException ex) {
				throw new Exception("Arguments missmatch!");
			}
		}
		return argMap;
	}
}

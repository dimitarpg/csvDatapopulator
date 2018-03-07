package com.proxiad.comdirect.util.csvtool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.proxiad.comdirect.util.csvtool.db.ComdirectDAO;
import com.proxiad.comdirect.util.csvtool.file.CsvFileProcessor;
import com.proxiad.comdirect.util.csvtool.file.FileProcessor;
import com.proxiad.comdirect.util.csvtool.file.SqlFileProcessor;

public class CsvTool {
	private static Properties appProperties = new Properties();
	private static SimpleAppLogger appLogger = SimpleAppLogger.getInstance();

	private static final String PARAM_CLEAR = "--clear";
	private static final String PARAM_VERBOSE = "--verbose";
	private static final String PARAM_VERBOSE_LEVEL_2 = "--verbose-2";
	private static final String PARAM_DISABLE_TRANSACTIONS = "--disable-transactions";
	private static final String PARAM_HELP = "--help";

	private static final String R_PARAM_TABLE = "-t";
	private static final String R_PARAM_OPERATION = "-op";
	private static final String R_PARAM_USER = "-u";
	private static final String R_PARAM_PASS = "-p";
	private static final String R_PARAM_ADDRESS = "-a";
	private static final String R_PARAM_FILE = "-f";

	private static final String DB_EXPORT_ACTION = "dbExport";
	private static final String SQL_IMPORT_ACTION = "sqlImport";
	private static final String CSV_IMPORT_ACTION = "csvImport";
	// TODO several threads for writing in the db

	public static void main(String[] args) {
		try {
			Map<String, String> cmdArguments = processCommandLineArguments(args);
			appLogger.logInfo("========================================================================");
			appLogger.logInfo("CVS Tool START");
			appProperties.load(CsvTool.class.getClassLoader().getResourceAsStream("app.properties"));

			if (cmdArguments.get(PARAM_HELP) != null) {
				appLogger.logInfo(appProperties.get("toolHelpMessage"));
			} else {
				ComdirectDAO cDao = new ComdirectDAO(appProperties, cmdArguments);
				FileProcessor fileProcessor;
				String appOperation = cmdArguments.get(R_PARAM_OPERATION);

				if (appOperation.equalsIgnoreCase(DB_EXPORT_ACTION)) {
					appLogger.logInfo("[EXPORT DB DATA]");
					fileProcessor = new CsvFileProcessor(appProperties, cmdArguments);

					appLogger.logInfo("start db extraction--");
					Map<String, List<String>> dbData = cDao.readDbData();
					appLogger.logInfo("finish db extraction--");

					appLogger.logInfo("start csv creation--");
					fileProcessor.writeToFile(dbData);
					appLogger.logInfo("start csv creation--");

				} else if (appOperation.equalsIgnoreCase(CSV_IMPORT_ACTION)) {
					fileProcessor = new CsvFileProcessor(appProperties, cmdArguments);
					appLogger.logInfo("[IMPORT CSV FILE]");

					appLogger.logInfo("start csv processign--");
					Map<String, List<String>> parcedCSVdoc = fileProcessor.readFile();
					appLogger.logInfo("finish csv processign--");

					appLogger.logInfo("start db population--");
					cDao.writeDbData(parcedCSVdoc);
					appLogger.logInfo("finish db population--");
				} else if (appOperation.equalsIgnoreCase(SQL_IMPORT_ACTION)) {
					appLogger.logInfo("[IMPORT SQL FILE]");

					appLogger.logInfo("start sql processign--");
					fileProcessor = new SqlFileProcessor(appProperties, cmdArguments);
					Map<String, List<String>> data = fileProcessor.readFile();
					appLogger.logInfo("finish sql processign--");

					appLogger.logInfo("start db population--");
					cDao.executeSQLStatements(data.get("sqlStatements"));
					appLogger.logInfo("finish db population--");
				}
			}

			appLogger.logInfo("===SUCCESS==");
		} catch (Exception e) {
			appLogger.logInfo("===FAIL==");
			e.printStackTrace();
			System.exit(0);
		} finally {
			appLogger.logInfo("CVS Tool FINISH");
			appLogger.logInfo("========================================================================");
		}
	}

	private static Map<String, String> processCommandLineArguments(String[] cmdArguments) throws Exception {
		Map<String, String> argMap = new HashMap<String, String>();
		if (cmdArguments.length == 0) {
			throw new Exception("No cmd argument provided!");
		} else {
			ArrayList<String> cmdArgumentsList = new ArrayList<String>(Arrays.asList(cmdArguments));
			try {
				String key;
				String value;
				if (cmdArgumentsList.contains(PARAM_VERBOSE)) {
					argMap.put(PARAM_VERBOSE, "YES!");
					appLogger.setVerboseEnable(true);
				}
				if (cmdArgumentsList.contains(PARAM_VERBOSE_LEVEL_2)) {
					argMap.put(PARAM_VERBOSE, "YES!");
					appLogger.setVerboseLevel(2);
				}
				if (cmdArgumentsList.contains(PARAM_HELP)) {
					argMap.put(PARAM_HELP, "YES!");
					return argMap;
				}

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
					key = cmdArgumentsList.get(cmdArgumentsList.indexOf(R_PARAM_ADDRESS));
					value = cmdArgumentsList.get(cmdArgumentsList.indexOf(R_PARAM_ADDRESS) + 1);
					argMap.put(key, value);
				} else {
					throw new Exception("Missing argument:-a");
				}
				if (cmdArgumentsList.contains(R_PARAM_OPERATION)) {
					key = cmdArgumentsList.get(cmdArgumentsList.indexOf(R_PARAM_OPERATION));
					value = cmdArgumentsList.get(cmdArgumentsList.indexOf(R_PARAM_OPERATION) + 1);
					argMap.put(key, value);
				} else {
					throw new Exception("Missing argument: -op");
				}
				if (cmdArgumentsList.contains(R_PARAM_TABLE)) {
					key = cmdArgumentsList.get(cmdArgumentsList.indexOf(R_PARAM_TABLE));
					value = cmdArgumentsList.get(cmdArgumentsList.indexOf(R_PARAM_TABLE) + 1);
					argMap.put(key, value);
				} else {
					// -t is not required argument if the operation is SQL_IMPORT_ACTION
					if (!argMap.get(R_PARAM_OPERATION).equalsIgnoreCase(SQL_IMPORT_ACTION)) {
						throw new Exception("Missing argument: -t");
					}
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
				if (cmdArgumentsList.contains(PARAM_DISABLE_TRANSACTIONS)) {
					argMap.put(PARAM_DISABLE_TRANSACTIONS, "YES!");
				}
			} catch (IndexOutOfBoundsException ex) {
				throw new Exception("Arguments missmatch!");
			}
		}
		return argMap;
	}
}

package com.proxiad.comdirect.util.csvtool.file;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

public class SqlFileProcessor extends FileProcessor {

	private static final String END_OF_STATEMENT = ">";
	private static final String MULTILINE_COMMENT_REGEX = "/\\*[^*]*\\*+(?:[^/*][^*]*\\*+)*/";

	public SqlFileProcessor(Properties appProps, Map<String, String> cmdArguments) throws IOException {
		this.appProperties = appProps;
		this.cmdArguments = cmdArguments;
	}

	@Override
	public Map<String, List<String>> readFile() throws Exception {
		Scanner sqlFileReader = null;
		StringBuilder sqlStatementBuilder = new StringBuilder();
		Map<String, List<String>> processedSqlFile = null;

		try {
			sqlFileReader = new Scanner(new File(this.cmdArguments.get(this.appProperties.get("rParamFile"))));
			processedSqlFile = new HashMap<String, List<String>>();

			String fileLine = null;
			// reads the whole file omitting the one line comments
			while (sqlFileReader.hasNext()) {
				fileLine = sqlFileReader.nextLine();

				// one line comment
				if (fileLine.startsWith("--")) {
					continue;
				}

				sqlStatementBuilder.append(fileLine);
				if (fileLine.endsWith(";")) {
					sqlStatementBuilder.append(END_OF_STATEMENT);
				}
			}

			String processedSQLFile = sqlStatementBuilder.toString();
			if (this.appLogger.getVerboseLevel() > 2) {
				this.appLogger.logInfo(processedSQLFile);
			}

			// removes the multiline-comments
			List<String> finalSqlStatements = new ArrayList<String>();
			for (String filteredSqlStatements : sqlStatementBuilder.toString().split(MULTILINE_COMMENT_REGEX)) {
				if (filteredSqlStatements.length() != 0) {
					String[] pSqlStatemets = filteredSqlStatements.split(";" + END_OF_STATEMENT);
					finalSqlStatements.addAll(Arrays.asList(pSqlStatemets));

					if (this.appLogger.getVerboseLevel() > 1) {
						this.appLogger.logInfo("sql-chunks: " + Arrays.asList(pSqlStatemets));
					}
				}
			}

			processedSqlFile.put("sqlStatements", finalSqlStatements);
		} finally {
			if (sqlFileReader != null) {
				sqlFileReader.close();
			}
		}
		return processedSqlFile;
	}

	@Override
	public void writeToFile(Map<String, List<String>> data) {
		// TODO Auto-generated method stub
	}
}

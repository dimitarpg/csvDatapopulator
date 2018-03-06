package com.proxiad.comdirect.util.csvtool.file;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;

public class CsvFileProcessor extends FileProcessor {

	public CsvFileProcessor(Properties appProps, Map<String, String> cmdArguments) throws IOException {
		this.appProperties = appProps;
		this.cmdArguments = cmdArguments;
	}

	@Override
	public Map<String, List<String>> readFile() throws Exception {
		Scanner csvReader = null;
		String csvLine = null;
		List<String> csvRowCells = null;
		StringBuilder csvFileBuilder = new StringBuilder();
		Map<String, List<String>> parcedCSVdoc = null;
		try {
			csvReader = new Scanner(new File(this.cmdArguments.get(this.appProperties.get("rParamFile"))));
			parcedCSVdoc = new HashMap<String, List<String>>();

			// the column line in the file should be well formated
			if (csvReader.hasNext()) {
				csvLine = csvReader.nextLine();
				csvRowCells = Arrays.asList(csvLine.split(this.appProperties.getProperty("csvCellDelimiterRegex")));
				if (csvRowCells.size() > 0) {
					// removes the quotes from the first and the last element
					this.removeSymbolsFromACell(csvRowCells, 0, 1, csvRowCells.get(0).length());
					this.removeSymbolsFromACell(csvRowCells, csvRowCells.size() - 1, 0,
							csvRowCells.get(csvRowCells.size() - 1).length() - 1);
				}
				parcedCSVdoc.put("columnCells", csvRowCells);
				this.appLogger.logInfo("columns(" + csvRowCells.size() + ") read:" + csvRowCells);
			}

			int csvDataRow = 0;
			while (csvReader.hasNext()) {
				csvLine = csvReader.nextLine();
				csvFileBuilder.append(csvLine);
				if (csvLine.charAt(csvLine.length() - 1) == '"') {
					// A Valid end of the line
					csvFileBuilder.append(";");
				}

				if (this.appLogger.getVerboseLevel() > 2) {
					this.appLogger.logInfo("csv(" + csvDataRow + ") row read:" + csvLine);
				}

				csvDataRow++;
				if (csvDataRow % 100 == 0 && csvDataRow > 0) {
					this.appLogger.logInfo(csvDataRow + " lines read");
				}
			}

			this.appLogger.logInfo("Total " + csvDataRow + " lines read");
			csvRowCells = Arrays
					.asList(csvFileBuilder.toString().split(this.appProperties.getProperty("csvCellDelimiterRegex")));
			if (csvRowCells.size() > 0) {
				// removes the quotes from the first and the last element
				removeSymbolsFromACell(csvRowCells, 0, 1, csvRowCells.get(0).length());
				removeSymbolsFromACell(csvRowCells, csvRowCells.size() - 1, 0,
						csvRowCells.get(csvRowCells.size() - 1).length() - 2);
			}

			if (this.appLogger.getVerboseLevel() > 2) {
				this.appLogger.logInfo("csv data read:" + csvRowCells);
			}

			parcedCSVdoc.put("dataCells", csvRowCells);
		} finally {
			if (csvReader != null) {
				csvReader.close();
			}
		}
		return parcedCSVdoc;
	}

	@Override
	public void writeToFile(Map<String, List<String>> dbData) throws Exception {
		Writer writer = null;
		try {
			String csvFileName = this.cmdArguments.get(this.appProperties.getProperty("rParamFile"));
			writer = new OutputStreamWriter(new FileOutputStream(csvFileName), StandardCharsets.UTF_8);

			// 1. write the columns names in the first row of the cs-file
			List<String> columnsCell = dbData.get("dbColumns");
			String columnRow = createCsvRow(columnsCell);
			if (columnRow != null) {
				writer.write(columnRow);
			}

			// 2. write each result from the db in separate row in the csv-file
			int dataRowNumber = 0;
			while (dbData.get(String.valueOf(dataRowNumber)) != null) {
				List<String> dbDataRow = dbData.get(String.valueOf(dataRowNumber));
				String singleDataRow = createCsvRow(dbDataRow);
				if (singleDataRow != null) {
					writer.write(singleDataRow);
				}
				dataRowNumber++;
			}

			writer.flush();
		} finally {
			if (writer != null) {
				writer.close();
			}
		}
	}

	private String createCsvRow(List<String> dbRow) {
		String csvDefaultSeparator = this.appProperties.getProperty("csvCellDelimiter");
		StringBuilder rowBuilder = null;

		if (dbRow != null && dbRow.size() > 0) {
			rowBuilder = new StringBuilder();

			// also I could escape the \n \r in the csv-cell itself
			for (int i = 0; i < dbRow.size(); i++) {
				String singleCell = dbRow.get(i);

				// System.out.println(Arrays.asList("asdasdas\"\"\"\"asd3513".split("\"(\"\")+")));
				// System.out.println("asdasdas\"\"\"\"asd3513".replaceAll("\"", "\"\""));
				// TODO should be verified
				String formattedCell = singleCell.replaceAll("\"", "\"\"");
				// formattedCell = formattedCell.replaceAll(csvDefaultSeparator, "\"" +
				// csvDefaultSeparator);

				rowBuilder.append("\"");
				rowBuilder.append(formattedCell);
				rowBuilder.append("\"");
				if (i < dbRow.size() - 1) {
					rowBuilder.append(csvDefaultSeparator);
				} else {
					rowBuilder.append("\n");
				}
			}

			return rowBuilder.toString();
		} else {
			return null;
		}
	}

	private void removeSymbolsFromACell(List<String> cells, int position, int startIndex, int endIndex) {
		if (cells.size() > 0) {
			String csvCell = cells.get(position);
			String formatedCsvCell = csvCell.substring(startIndex, endIndex);
			cells.set(position, formatedCsvCell);
		}
	}
}

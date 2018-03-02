package com.proxiad.comdirect.util.csvtool;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ComdirectDAO {

	private Properties appProperties;

	public ComdirectDAO(Properties props) throws IOException {
		this.appProperties = props;
	}

	public Map<String, List<String>> readDbData(Map<String, String> appInstArguments) throws Exception {
		Connection connection = null;
		PreparedStatement selectAllStatement = null;
		ResultSet resultSet = null;
		ResultSetMetaData rsmd = null;

		Map<String, List<String>> dbData = new HashMap<String, List<String>>();
		try {
			JdbcConnection jdbcConnection = JdbcConnection.getInstance(
					appInstArguments.get(appProperties.get("rParamAddress")),
					appInstArguments.get(appProperties.get("rParamUser")),
					appInstArguments.get(appProperties.get("rParamPass")));

			connection = jdbcConnection.getConnection();

			String stmt = String.format(" SELECT * FROM %s ", appInstArguments.get(appProperties.get("rParamTable")));
			selectAllStatement = connection.prepareStatement(stmt);
			resultSet = selectAllStatement.executeQuery(stmt);

			rsmd = resultSet.getMetaData();
			int columnsNumber = rsmd.getColumnCount();

			int i = 1;
			List<String> columns = new ArrayList<String>();
			Map<String, Integer> columnTypeMap = new HashMap<String, Integer>();
			while (i <= columnsNumber) {
				columns.add(rsmd.getColumnName(i));
				columnTypeMap.put(rsmd.getColumnName(i), rsmd.getColumnType(i));
				i++;
			}

			if (columns != null && columns.size() > 0) {
				dbData.put("dbColumns", columns);
				SimpleDateFormat cvsDateFormat = new SimpleDateFormat(appProperties.getProperty("csvDateFormatIn"));

				int dbRowNumber = 0;
				while (resultSet.next()) {
					List<String> row = new ArrayList<String>();

					for (String columnName : columns) {
						String cellData = null;
						switch (columnTypeMap.get(columnName)) {
						case Types.DATE: {
							Date date = resultSet.getDate(columnName);
							if (date != null) {
								cellData = cvsDateFormat.format(date);
							} else {
								cellData = "";
							}
							break;
						}
						case Types.TIMESTAMP:
						case Types.TIMESTAMP_WITH_TIMEZONE: {
							Timestamp timestamp = resultSet.getTimestamp(columnName);
							if (timestamp != null) {
								cellData = cvsDateFormat.format(timestamp);
							} else {
								cellData = "";
							}
							break;
						}
						case Types.TIME: {
							Time time = resultSet.getTime(columnName);
							cellData = cvsDateFormat.format(time);
							break;
						}
						default: {
							String defaultCell = resultSet.getString(columnName);
							if (defaultCell != null) {
								cellData = defaultCell;
							} else {
								cellData = "";
							}
							break;
						}
						}
						row.add(cellData);
					}
					dbData.put(String.valueOf(dbRowNumber), row);
					dbRowNumber++;
				}
			}

			return dbData;
		} finally {
			JdbcConnection.closeConnection(connection, selectAllStatement, resultSet);
		}

	}

	public void writeDbData(Map<String, String> arguments, Map<String, List<String>> data) throws Exception {
		String stmt = null;
		Connection connection = null;
		PreparedStatement insertSt = null;
		try {
			JdbcConnection jdbcConnection = JdbcConnection.getInstance(
					arguments.get(appProperties.get("rParamAddress")), arguments.get(appProperties.get("rParamUser")),
					arguments.get(appProperties.get("rParamPass")));

			connection = jdbcConnection.getConnection();

			if (arguments.get(appProperties.get("paramClear")) != null) {
				stmt = "DELETE FROM " + arguments.get(appProperties.get("rParamTable"));
				insertSt = connection.prepareStatement(stmt);
				insertSt.executeUpdate();
			}
			connection.setAutoCommit(false);

			String columnCells = prepareSqlStatementValues(data.get("columnCells"), false);

			int numberOfColumns = data.get("columnCells").size();
			List<String> dataCells = data.get("dataCells");
			List<String> insertStatementValues;

			for (int i = 0; i < dataCells.size(); i = i + numberOfColumns) {
				insertStatementValues = dataCells.subList(i, i + numberOfColumns);

				String insertValues = prepareSqlStatementValues(insertStatementValues, true);

				stmt = (String) appProperties.get("insertStatement");
				stmt = String.format(stmt, arguments.get(appProperties.get("rParamTable")), columnCells, insertValues);
				insertSt = connection.prepareStatement(stmt);
				// logInfo("execute insert statemet{" + i + "}: " + insertSt.toString());
				insertSt.executeUpdate();
			}
			connection.commit();
		} catch (Exception e) {
			connection.rollback();
			throw new Exception("Statement: '" + stmt + "' caused by:", e);
		} finally {
			JdbcConnection.closeConnection(connection, insertSt, null);
		}
	}

	private String prepareSqlStatementValues(List<String> values, boolean surroundWithSingleQuotes) {
		if (values != null && values.size() > 0) {
			SimpleDateFormat cvsDateFormat = new SimpleDateFormat(appProperties.getProperty("csvDateFormatIn"));
			StringBuilder valuesBuilder = new StringBuilder();
			for (int i = 0; i < values.size(); i++) {
				String value = values.get(i);

				// escapes the single quote(') in the cell
				String escapedValue = value.replaceAll("'", "''");

				escapedValue = escapedValue.replaceAll("\"", "");

				try {
					cvsDateFormat.parse(escapedValue);
					valuesBuilder.append(" TO_DATE('" + escapedValue + "', '"
							+ appProperties.getProperty("csvDateFormatOut") + "') ");
				} catch (ParseException e) {
					if (surroundWithSingleQuotes) {
						valuesBuilder.append("'" + escapedValue + "'");
					} else {
						valuesBuilder.append(escapedValue);
					}
				}
				if (i != values.size() - 1) {
					valuesBuilder.append(",");
				}
			}
			return valuesBuilder.toString();
		} else {
			return null;
		}
	}

}

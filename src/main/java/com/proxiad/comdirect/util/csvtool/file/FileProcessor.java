package com.proxiad.comdirect.util.csvtool.file;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.proxiad.comdirect.util.csvtool.SimpleAppLogger;

public abstract class FileProcessor {

	protected SimpleAppLogger appLogger = SimpleAppLogger.getInstance();
	protected Properties appProperties;
	protected Map<String, String> cmdArguments;

	public abstract Map<String, List<String>> readFile() throws Exception;

	public abstract void writeToFile(Map<String, List<String>> data) throws Exception;
}

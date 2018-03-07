package com.proxiad.comdirect.util.csvtool;

public class SimpleAppLogger {

	private static SimpleAppLogger instance;

	private boolean verboseEnable = false;
	private int verboseLevel = 1;

	private SimpleAppLogger() {
	}

	public static SimpleAppLogger getInstance() {
		if (instance == null) {
			instance = new SimpleAppLogger();
		}
		return instance;
	}

	public void logInfo(Object string) {
		if (this.verboseEnable) {
			System.out.println(string);
		}
	}

	public void logError(Throwable t) {
		if (t != null) {
			System.out.println(t.getMessage());
		}
		if (t.getCause() != null) {
			System.out.println("Caused by:");
			System.out.println(t.getCause().getMessage());
		}
	}

	public int getVerboseLevel() {
		return verboseLevel;
	}

	public void setVerboseLevel(int verboseLevel) {
		this.verboseLevel = verboseLevel;
	}

	public void setVerboseEnable(boolean verboseEnable) {
		this.verboseEnable = verboseEnable;
	}

}
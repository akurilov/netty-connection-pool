package com.github.akurilov.netty.connection.pool.test.util;

import java.io.IOException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by olga on 08.07.15.
 */
public interface PortTools {

	String FMT_PATTERN_CONN = "\\s+ESTABLISHED";
	
	static Scanner getNetstatOutput()
	throws IOException {
		final var netstatCommand = new String[]{ "netstat", "-an" };
		final var netstatProcess = Runtime.getRuntime().exec(netstatCommand);
		return new Scanner(netstatProcess.getInputStream(), "IBM850").useDelimiter("\\n");
	}
	
	static int getConnectionCount(final String nodeAddrWithPort)
	throws IOException {
		var countConnections = 0;
		final var patternConn = Pattern.compile(nodeAddrWithPort + FMT_PATTERN_CONN);
		try(final var netstatOutputScanner = getNetstatOutput()) {
			String line;
			Matcher m;
			while(netstatOutputScanner.hasNext()) {
				line = netstatOutputScanner.next();
				m = patternConn.matcher(line);
				if(m.find()) {
					countConnections ++;
				}
			}
		}
		return countConnections;
	}
}

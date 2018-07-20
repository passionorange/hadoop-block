package passionorange.demo;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Parses the log file based on fixed regex pattern, meant to parse
 * <p>
 * 221.220.8.0 1341391325000 /index.html 200 140 Mozilla/5.0 (Windows; U;
 * Windows NT 6.1;rv:2.2) Gecko/20110201";
 * </p>
 * as
 * <p>
 * ip, timestamp, resource, size, loadTime andrequester information
 * </p>
 * 
 */
public class LogParser {

	// first matching group can have digits or dot, followed by space, followed by
	// set of digits, followed by space, followed by words, with / included...and so on.
	final static Pattern logPattern = Pattern.compile(
			"([\\d.]+)\\s+([\\d]+)\\s+([\\/\\w.]+)\\s+([\\d]+)\\s+([\\d]+)\\s+([\\w\\d\\s.\\/\\(\\);:][^\\n]+)");

	public class LogRecord {
		String ip;
		Long timeStamp;
		String resource;
		Long size;
		Long loadTime;
		String requester;

		@Override
		public String toString() {
			return "LogRecord [ip=" + ip + ", timeStamp=" + timeStamp + ", resource=" + resource + ", size=" + size
					+ ", loadTime=" + loadTime + ", requester=" + requester + "]";
		}

	}

	public List<LogParser.LogRecord> parse(String input_str) {

		ArrayList<LogRecord> arrayList = new ArrayList<LogParser.LogRecord>();
		// https://www.freeformatter.com/java-regex-tester.html
		Matcher matcher = logPattern.matcher(input_str);
		while (matcher.find()) {
			LogRecord lr = new LogParser.LogRecord();
			arrayList.add(lr);
			for (int i = 1; i <= matcher.groupCount(); i++) {
				String raw_value = matcher.group(i);
				System.out.println(raw_value);
				fillLogRecord(lr, i, raw_value);
			}
			System.out.println(lr);
		}
		return arrayList;
	}

	private void fillLogRecord(LogRecord lr, int i, String raw_value) {
		switch (i) {
		case 1:
			lr.ip = raw_value;
			break;
		case 2:
			lr.timeStamp = Long.valueOf(raw_value);
			break;
		case 3:
			lr.resource = raw_value;
			break;
		case 4:
			lr.size = Long.valueOf(raw_value);
			break;
		case 5:
			lr.loadTime = Long.valueOf(raw_value);
			break;
		case 6:
			lr.requester = raw_value;
		}
	}

	public static void main(String[] args) {
		String input = "221.220.8.0	1341391325000	/index.html	200	140	Mozilla/5.0 (Windows; U; Windows NT 6.1; rv:2.2) Gecko/20110201";
		new LogParser().parse(input);

		// parse();
	}

}

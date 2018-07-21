package passionorange.demo;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.google.common.io.Files;

import passionorange.demo.LogParser.LogRecord;

/**
 * Extracts the log record from the raw data
 */
public class IPLogMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	LogParser lp;
	List<String> counter_tokens;

	@Override
	public void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) {
		lp = new LogParser();
		try {
			URI[] cacheFiles = context.getCacheFiles();
			if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
				File keys_file = new File("./keys");
				List<String> readLines = Files.readLines(keys_file, Charset.defaultCharset());
				System.out.println("Counter Tokens:" + readLines);
				counter_tokens = readLines;
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void map(LongWritable key, Text value, Context context) {
		List<LogRecord> log0 = lp.parse(value.toString());
		try {
			for (String token : counter_tokens) {
				if (log0.get(0).resource.contains(token)) {
					context.getCounter("passionorange", token).increment(1l);
					break;
				}
			}
			context.write(new Text(log0.get(0).ip), new LongWritable(1));
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

}

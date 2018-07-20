package passionorange.demo;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import passionorange.demo.LogParser.LogRecord;

/**
 * Extracts the log record from the raw data
 */
public class IPLogMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

	LogParser lp;

	@Override
	public void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) {
		lp = new LogParser();
	}

	@Override
	public void map(LongWritable key, Text value, Context context) {
		List<LogRecord> log0 = lp.parse(value.toString());
		try {
			context.write(new Text(log0.get(0).ip), new LongWritable(1));
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

}

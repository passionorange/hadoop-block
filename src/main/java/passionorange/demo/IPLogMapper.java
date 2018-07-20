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

	static enum RecordCounters {
		resource_index, resource_cart, resource_about, resource_other
	};

	@Override
	public void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) {
		lp = new LogParser();
	}

	@Override
	public void map(LongWritable key, Text value, Context context) {
		List<LogRecord> log0 = lp.parse(value.toString());
		try {
			if(log0.get(0).resource.contains("index")) {
				context.getCounter(RecordCounters.resource_index).increment(1l);
			} else if (log0.get(0).resource.contains("cart")) {
				context.getCounter(RecordCounters.resource_cart).increment(1l);
			} else if(log0.get(0).resource.contains("about")) {
				context.getCounter(RecordCounters.resource_about).increment(1l);
			} else {
				context.getCounter(RecordCounters.resource_other).increment(1l);
			}
			context.write(new Text(log0.get(0).ip), new LongWritable(1));
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}

}

package passionorange.demo;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper to do regex matching and emit their counts, not thread safe 
 */
public class GrepMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	
	public static String PATTERN = "myregex.pattern";
	public static String GROUP = "myregex.group";
	//the regex pattern
	private Pattern pattern_to_search;
	//the group number of match
	private int group_number;
	
	/**
	 * Called once
	 */
	@Override
	public void setup(Mapper<LongWritable, Text, Text, LongWritable>.Context context) {
	      Configuration conf = context.getConfiguration();
	      this.pattern_to_search = Pattern.compile(conf.get(PATTERN));
	      this.group_number = conf.getInt(GROUP, 0);
	      System.out.println("Setup of Mapper Done, pattern:" + this.pattern_to_search);
	}

	@Override
	public void map(LongWritable key, Text value, Context context) {
		String input = value.toString();
		Matcher matcher = this.pattern_to_search.matcher(input);
		System.out.println("Processing:" + input);
		while(matcher.find()) {
			try {
				context.write(new Text(matcher.group(this.group_number)), new LongWritable(1L));
			} catch (IOException | InterruptedException e) {
				//no-op
			}
		}
	}

}

package passionorange.demo;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MaxTempMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	public void map(LongWritable key, Text value, Context context) {
		String line = value.toString();
		String year = line.substring(15, 19);
		int airTemperature = Integer.parseInt(line.substring(87, 92));
		try {
			System.out.println("Processing line: "+ line + "year: "+ year + "airtemperatrue: " + airTemperature);
			context.write(new Text(year), new IntWritable(airTemperature));
		} catch (IOException | InterruptedException e) {
			// no-op
		}
	}

}

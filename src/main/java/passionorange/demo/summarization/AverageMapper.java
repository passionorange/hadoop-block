package passionorange.demo.summarization;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class AverageMapper extends Mapper<LongWritable, Text, Text, AverageOfTuple>{

	@Override
	public void map(LongWritable key, Text value, Context context) {
		String[] line = value.toString().split(" ");
		if(line.length < 2) {
			return;
		}
		String kVal = line[0];
		Double dVal= Double.valueOf(line[1]);
		AverageOfTuple averageOfTuple = new AverageOfTuple();
		averageOfTuple.count = 1;
		averageOfTuple.average = dVal;
		try {
			context.write(new Text(kVal), averageOfTuple);
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}
}

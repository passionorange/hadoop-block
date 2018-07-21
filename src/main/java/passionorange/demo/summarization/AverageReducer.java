package passionorange.demo.summarization;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, AverageOfTuple, Text, AverageOfTuple> {

	@Override
	public void reduce(Text key, Iterable<AverageOfTuple> values, Context context) {
		int sum = 0;
		int count = 0;
		for (AverageOfTuple value : values) {
			sum += value.count * value.average;
			count += value.count;
		}
		AverageOfTuple averageOfTuple = new AverageOfTuple();
		averageOfTuple.average = sum / count;
		averageOfTuple.count = count;
		try {
			context.write(key, averageOfTuple);
		} catch (IOException | InterruptedException e) {
			e.printStackTrace();
		}
	}
}

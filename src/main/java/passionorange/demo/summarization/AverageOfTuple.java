package passionorange.demo.summarization;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Average is not associative, to find average to a combination of numbers whose
 * averages are individually known we need to also know the number of elements
 * which were used to calculate the average.
 */
public class AverageOfTuple implements Writable{

	double average;
	int count;
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeDouble(average);
		out.writeInt(count);
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		average = in.readDouble();
		count = in.readInt();
	}
	

	@Override
	public String toString() {
		return average + "\t" + count;
	}
}

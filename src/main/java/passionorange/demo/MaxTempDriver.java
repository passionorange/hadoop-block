package passionorange.demo;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Execute using 
 * hadoop jar target/uber-hadoop-block-0.1.jar passionorange.demo.MaxTempDriver 'hdfs://localhost:8020//sample.txt' 'hdfs://localhost:8020//map_reduce'
 */
public class MaxTempDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "Max Temp Driver");
		job.setJarByClass(getClass());
		// FileInputFormat.setInputPath(job, "/sample.txt");
		// FileOutputFormat.setOutputPath(job, new Path("/"));
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setMapperClass(MaxTempMapper.class);
		job.setCombinerClass(MaxTempReducer.class);
		job.setReducerClass(MaxTempReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MaxTempDriver(), args);
		System.exit(exitCode);
	}

}

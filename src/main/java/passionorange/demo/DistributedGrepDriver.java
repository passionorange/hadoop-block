package passionorange.demo;

import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Execute using 
 * hadoop jar target/uber-hadoop-block-0.1.jar passionorange.demo.DistributedGrepDriver 'hdfs:////sample.txt' 'FM' 0  'hdfs:////distributed-grep-temp'
 *
 */
public class DistributedGrepDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if(args.length < 3) {
			System.out.println("Usage DistributedGrepDriver <inputDir> <regular expression> <matching group> <output-directory>");
			return 1;
		}
		
		Path tempDir =  new Path(args[3] + "-" + Integer.toString(new Random().nextInt(50)));
		
		Configuration conf = getConf();
		System.out.println("Conf:" + conf);
		conf.set(GrepMapper.PATTERN, args[1]);
		conf.set(GrepMapper.GROUP, args[2]);

		Job grepJob = Job.getInstance(getConf(), "Distributed Grep Job");
		grepJob.setJarByClass(getClass());
		FileInputFormat.setInputPaths(grepJob, args[0]);
		FileOutputFormat.setOutputPath(grepJob, tempDir);
		grepJob.setMapperClass(GrepMapper.class);
		//careful for imports
		grepJob.setCombinerClass(LongSumReducer.class);
		grepJob.setReducerClass(LongSumReducer.class);
		grepJob.setOutputKeyClass(Text.class);
		grepJob.setOutputValueClass(LongWritable.class);
		// if you uncommment following line, you will get exception that job state is in define
		//System.out.println("Job:" + grepJob);
		return grepJob.waitForCompletion(true) ? 0 : 1;
		
	}
	
	public static void main(String[] args) {
		DistributedGrepDriver distributedGrepDriver = new DistributedGrepDriver();
		try {
			ToolRunner.run(distributedGrepDriver, args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

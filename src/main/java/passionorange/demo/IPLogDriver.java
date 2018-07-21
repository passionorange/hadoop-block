package passionorange.demo;

import java.net.URI;
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
 * hadoop jar target/uber-hadoop-block-0.1.jar passionorange.demo.IPLogDriver 'hdfs:////test_input/access_logs.txt'  'hdfs:////test_output'
 * 
 * Returns the number of times an IP Address has accessed any of the resource
 */
public class IPLogDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if(args.length < 2) {
			System.out.println("Usage IPLogDriver <inputDir> <output-directory>");
			return 1;
		}
		Path tempDir =  new Path(args[1] + "/"+ "ip_log_output" +"-" + Integer.toString(new Random().nextInt(50)));
		Configuration conf = getConf();
		System.out.println("Conf:" + conf);
		Job ljob = Job.getInstance(getConf(), "IP Log Driver");
		ljob.setJarByClass(getClass());
		FileInputFormat.setInputPaths(ljob, args[0]);
		FileOutputFormat.setOutputPath(ljob, tempDir);
		ljob.setMapperClass(IPLogMapper.class);
		//careful for imports
		ljob.setCombinerClass(LongSumReducer.class);
		ljob.setReducerClass(LongSumReducer.class);
		ljob.setOutputKeyClass(Text.class);
		ljob.setOutputValueClass(LongWritable.class);
		//below is equivalent to conf.set("mapreduce.job.cache.files", files == null ? uri.toString() : files + "," + uri.toString());
		//add a cache file
		ljob.addCacheFile(new URI("hdfs://localhost:8020/test_input/resource_types.txt#keys"));
		return ljob.waitForCompletion(true) ? 0 : 1;
	}
	
	/**
	 * Note that main should not just call run, it should call via ToolRunner,
	 * else you will get null from getConf()
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		IPLogDriver ipLogDriver = new IPLogDriver();
		try {
			ToolRunner.run(ipLogDriver, args);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}

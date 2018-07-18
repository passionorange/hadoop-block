package passionorange.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Execute using hadoop jar target/uber-hadoop-block-0.1.jar
 * passionorange.demo.ConfigurationPrinter
 */
public class ConfigurationPrinter extends Configured implements Tool {

	static {
		Configuration.addDefaultResource("hdfs-default.xml");
		Configuration.addDefaultResource("hdfs-site.xml");
		Configuration.addDefaultResource("mapred-default.xml");
		Configuration.addDefaultResource("mapred-site.xml");
	}

	public static void main(String[] args) {
		try {
			ToolRunner.run(new ConfigurationPrinter(), args);
		} catch (Exception e) {
			// no-op
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		Configuration conf = getConf();
		System.out.println(conf);
		for (java.util.Map.Entry<String, String> entry : conf) {
			System.out.println(entry.getKey() + ":" + entry.getValue());
		}
		return 0;
	}

}

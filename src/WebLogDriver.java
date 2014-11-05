import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WebLogDriver extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		int res = ToolRunner.run(conf, new WebLogDriver(), args);
		System.exit(res);
	}

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = this.getConf();

		conf.set(WebLogConstants.fieldTerminatorProp, WebLogConstants.fieldTerminator);
		conf.set(WebLogConstants.sessionExpireTimeProp, String.valueOf(WebLogConstants.sessionExpire));
		
		/* getting the input file */
		Job job = Job.getInstance(conf, "WebLog Processing");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		
		/* Setting driver and mapper class */
		job.setJarByClass(WebLogDriver.class);
		job.setMapperClass(WebLogMapper.class);

		job.setNumReduceTasks(1);
		
		/* output classes to get key and value */
		job.setMapOutputKeyClass(WebLogKey.class);
		job.setMapOutputValueClass(WebLogVal.class);
		
		/* Setting the reducer class */
		job.setReducerClass(WebLogReducer.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return (job.waitForCompletion(true) ? 0 : 1);
	}
}

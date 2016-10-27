package hadoop_test1.test.config;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import hadoop_test1.test.mapper.RecordSumMapper;
import hadoop_test1.test.reducer.ReducerSum;

public class ExecuterTwo extends Configured implements Tool{

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new ExecuterOne(), args);
		System.exit(exitCode);
	}
	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(getConf(), "Executer One - some operation, not sure what, yet");
		
		job.setJarByClass(getClass());

		FileInputFormat.setInputPaths(job, new Path(args[0]));

		job.setMapperClass(RecordSumMapper.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setReducerClass(ReducerSum.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

}

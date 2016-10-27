package hadoop_test1.test.mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RecordSumMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	private Text word = new Text();
	private LongWritable count = new LongWritable();

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		// context.write(value, new IntWritable());
		context.write(new Text("count"), new LongWritable(1));

		String[] split = value.toString().split("\t+");
		word.set(split[0]);	
		if (split.length > 2) {
			try {
				count.set(Long.parseLong(split[1]));
				context.write(word, count);
			} catch (NumberFormatException e) {
				// cannot parse - ignore
			}
		}
	}

}

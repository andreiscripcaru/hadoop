package hadoop_test1.test.reducer;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerSum extends Reducer<Text, LongWritable, Text, LongWritable> {

	@Override
	protected void reduce(Text key, Iterable<LongWritable> records, Context context) throws IOException, InterruptedException {
		long sum = 0;
		
		for (LongWritable rec : records){
			sum +=rec.get();
		}
		
		context.write(key, new LongWritable(sum));
	}

}

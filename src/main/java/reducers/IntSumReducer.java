package reducers;
/*
 * IntSumReducer
 * 
 * 
 * @author Edivaldo Mascarenhas Jr.
 * @version 1.0
*/
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The Class IntSumReducer.
 */
public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	private IntWritable result = new IntWritable();

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
	 */
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int sum = 0;
		for (IntWritable val : values) {
			sum += val.get();
		}

		result.set(sum);
		context.write(key, result);

	}
}

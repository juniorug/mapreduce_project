package mappers;
/*
 * TopTenBrazilianWritersMapper
 * 
 * 
 * @author Edivaldo Mascarenhas Jr.
 * @version 1.0
*/
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.MRDPUtils;

/**
 * The Class TopTenBrazilianWritersMapper.
 */
public class TopTenBrazilianWritersMapper extends Mapper<Object, Text, NullWritable, Text> {

	private final static IntWritable ONE = new IntWritable(1);

	/* (non-Javadoc)
	 * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
	 */
	@Override
	protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		Map<String, String> map = MRDPUtils.transformXmlToMap(value.toString());

		if ((map.get("Id") != null) && (map.get("DisplayName") != null) && (map.get("Reputation") != null)) {
			Text texto = new Text(map.get("DisplayName") + "|" + map.get("Id") + "|" + map.get("Reputation") + "|"
					+ map.get("Location") + "\t" + ONE);
			context.write(NullWritable.get(), texto);
		}
	}

}
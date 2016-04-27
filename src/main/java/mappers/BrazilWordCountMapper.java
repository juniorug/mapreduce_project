package mappers;

import java.io.IOException;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.MRDPUtils;

public class BrazilWordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		// Parse the input string into a nice map
		Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());

		// Grab the "Text" field, since that is what we are counting over
		String txt = parsed.get("Location");

		// .get will return null if the key is not there
		if (txt == null) {
			// skip this record
			return;
		}

		// Unescape the HTML because the SO data is escaped.
		txt = StringEscapeUtils.unescapeHtml(txt.toLowerCase());

		// Remove some annoying punctuation
		txt = txt.replaceAll("'", ""); // remove single quotes (e.g., can't)
		txt = txt.replaceAll("[^a-zA-Z]", " "); // replace the rest with a
		// space

		// Tokenize the string, then send the tokens away
		StringTokenizer itr = new StringTokenizer(txt);
		while (itr.hasMoreTokens()) {
			word.set(itr.nextToken());
			context.write(word, one);
		}
	}
}
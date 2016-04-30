package reducers;
/*
 * TopTenBrazilianWritersReducer
 * 
 * 
 * @author Edivaldo Mascarenhas Jr.
 * @version 1.0
*/
import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import util.StringUtils;
import util.UserComparator;

/**
 * The Class TopTenBrazilianWritersReducer.
 */
public class TopTenBrazilianWritersReducer extends Reducer<NullWritable, Text, Text, Text> {
    
    private TreeMap<UserComparator, Text> map = new TreeMap<UserComparator, Text>(new UserComparator(null, null, null));
    private Text value = new Text();

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text text : values) {
            String[] dataSplit = text.toString().split("\t");
            String[] userdata =  dataSplit[0].split("\\|");
            String name = userdata[0];
            Integer reputation = Integer.parseInt(userdata[2]); 

            value.set(StringUtils.formatter(text));
            map.put(new UserComparator(name, reputation, Integer.parseInt(dataSplit[1])), new Text(value));
            if (map.size() > 10) {
                map.remove(map.firstKey());
            }

        }

    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Reducer#cleanup(org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
    	context.write(new Text(StringUtils.getLine()), new Text());
        for (UserComparator data : map.descendingKeySet()) {
            value.set(data.getCount().toString());
            Text numposts = new Text("Post counter: " + value);
            context.write(map.get(data), numposts);
        }
        context.write(new Text(StringUtils.getLine()), new Text());
    }
    
    
}

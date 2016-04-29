package reducers;
/*
 * TopTenBrazilianWritersCombiner
 * 
 * 
 * @author Edivaldo Mascarenhas Jr.
 * @version 1.0
*/
import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import util.UserComparator;

/**
 * The Class TopTenBrazilianWritersCombiner.
 */
public class TopTenBrazilianWritersCombiner extends Reducer<NullWritable, Text, NullWritable, Text> {
    
    private TreeMap<UserComparator, Text> map = new TreeMap<UserComparator, Text>(new UserComparator(null, null, null));
    private HashMap<String, Integer> dataCountMap = new HashMap<String, Integer>();

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text texto : values) {
            String[] dataSplit = texto.toString().split("\t");  
            String data = dataSplit[0];     
            if (dataCountMap.containsKey(data)) {
                dataCountMap.put(data, dataCountMap.get(data) + 1);  
            } else {
                dataCountMap.put(data, 1); 
            }

        }
        for (String data : dataCountMap.keySet()) {   
            int total = dataCountMap.get(data);     
            String[] userdata =  data.split("\\|");            
            String name = userdata[0];   
            Integer id = Integer.parseInt(userdata[1]);   
            Integer reputation = Integer.parseInt(userdata[2]);   
            map.put(new UserComparator(name, reputation, total), new Text(data + "\t" + total));
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
        for (Text t : map.values()) {
            context.write(NullWritable.get(), t);
        }

    }

}

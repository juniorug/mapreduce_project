/*
 * 
 */
package mappers;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.MRDPUtils;

// TODO: Auto-generated Javadoc
/**
 * The Class FilterMapper.
 */
public class FilterMapper extends Mapper<Object, Text, NullWritable, Text> {
    
    /** The map regex. */
    private String mapRegex = null;
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#setup(org.apache.hadoop.mapreduce.Mapper.Context)
     */
    public void setup(Context context) throws IOException, InterruptedException {
        mapRegex = context.getConfiguration().get("mapRegex");
    }
    
    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        Map<String, String> map = MRDPUtils.transformXmlToMap(value.toString());
        String entry = map.get("Location");
        if (( entry != null ) && ( entry.toLowerCase().matches(mapRegex) )) {
                context.write(NullWritable.get(), value);
        }
    }
    
}


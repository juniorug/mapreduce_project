package mappers;
/*
 * UserMapper
 * 
 * 
 * @author Edivaldo Mascarenhas Jr.
 * @version 1.0
*/
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.MRDPUtils;

/**
 * The Class UserMapper.
 */
public class UserMapper extends Mapper<Object, Text, Text, Text> {
    
    private Text outkey = new Text();
    private Text outvalue = new Text();
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
        if ((entry != null) && (entry.toLowerCase().matches(mapRegex))) {

            String userId = map.get("Id");
            if (userId != null) {
                outkey.set(userId);
                outvalue.set("A" + value);
                context.write(outkey, outvalue);
            }
        }

    }
}
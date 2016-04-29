/*
 * 
 */
package mappers;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.MRDPUtils;

// TODO: Auto-generated Javadoc
/**
 * The Class PostMapper.
 */
public class PostMapper extends Mapper<Object, Text, Text, Text> {
    
    /** The outkey. */
    private Text outkey = new Text();
    
    /** The outvalue. */
    private Text outvalue = new Text();

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        Map<String, String> map = MRDPUtils.transformXmlToMap(value.toString());
        String userId = map.get("OwnerUserId");
        if (userId != null) {
            outkey.set(userId);
            outvalue.set("B" + value.toString());
            context.write(outkey, outvalue);
        }
    }

}
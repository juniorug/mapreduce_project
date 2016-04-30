package mappers;
/*
 * BadgesMapper
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
 * The Class BadgesMapper.
 */
public class BadgesMapper  extends Mapper<Object, Text, Text, Text> {
    
    private Text outkey = new Text();
    private Text outvalue = new Text();

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        Map<String, String> map = MRDPUtils.transformXmlToMap(value.toString());
        String userId = map.get("UserId");
        if (userId != null) {
            outkey.set(userId);
            outvalue.set("B" + value.toString());
            context.write(outkey, outvalue);
        }
    }

}
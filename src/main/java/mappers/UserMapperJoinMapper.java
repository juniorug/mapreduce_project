package mappers;
/*
 * UserMapperJoinMapper
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
 * The Class UserMapperJoinMapper.
 */
public class UserMapperJoinMapper extends Mapper<Object, Text, Text, Text> {
    private Text outkey = new Text();
    private Text outvalue = new Text();

    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Mapper#map(KEYIN, VALUEIN, org.apache.hadoop.mapreduce.Mapper.Context)
     */
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        Map<String, String> map = MRDPUtils.transformXmlToMap(value.toString());
        String id = map.get("Id");
        if (id != null) {
            outkey.set(id);
            outvalue.set("A" + formatData(value));
            context.write(outkey, outvalue);
        }
    }
    
    private String formatData(Text a) {
        Map<String, String> map = MRDPUtils.transformXmlToMap(a.toString());
        StringBuilder sb = new StringBuilder();
        sb.append("<row UserId=\"").append(map.get("Id")).append("\" DisplayName=\"").append(map.get("DisplayName")).append("\" />");
        return sb.toString();
    }
}
package mappers;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.MRDPUtils;

public class FilterMapper extends Mapper<Object, Text, NullWritable, Text> {
    private String mapRegex = null;
    
    public void setup(Context context) throws IOException, InterruptedException {
        mapRegex = context.getConfiguration().get("mapRegex");
    }
    
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        
        Map<String, String> map = MRDPUtils.transformXmlToMap(value.toString());
        String entry = map.get("Location");
        if (( entry != null ) && ( entry.toLowerCase().matches(mapRegex) )) {
                context.write(NullWritable.get(), value);
        }
    }
    
}


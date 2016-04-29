package mappers;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import util.MRDPUtils;

public class TopTenBrazilianWritersMapper extends Mapper<Object, Text, NullWritable, Text> {

    private final static IntWritable ONE = new IntWritable(1);

    @Override
    protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

        Map<String, String> map = MRDPUtils.transformXmlToMap(value.toString());

        if ((map.get("Id") != null) && (map.get("DisplayName") != null)) {
            Text texto = new Text( map.get("DisplayName") + "|"  + map.get("Id") +"\t" + ONE);
            System.out.println(texto.toString());
            context.write(NullWritable.get(), texto);
        }
    }

}
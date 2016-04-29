package reducers;

import java.io.IOException;
import java.util.HashMap;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import util.UserComparator;


public class TopTenBrazilianWritersCombiner extends Reducer<NullWritable, Text, NullWritable, Text> {
    private TreeMap<UserComparator, Text> map = new TreeMap<UserComparator, Text>(new UserComparator(null, null, null));
    private HashMap<String, Integer> dataCountMap = new HashMap<String, Integer>();

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text texto : values) {
        	// texto = 'Fabio Gouw|250329	1'
            String[] dataSplit = texto.toString().split("\t");  
            //dataSplit[0] = TFabio Gouw|250329
            //dataSplit[1] = 1
            String data = dataSplit[0];     //DATA = Fabio Gouw|250329
            if (dataCountMap.containsKey(data)) {
                dataCountMap.put(data, dataCountMap.get(data) + 1);  // dataCountMap("Fabio Gouw|250329", 1 +1)
            } else {
                dataCountMap.put(data, 1);   // dataCountMap("Fabio Gouw|250329", 1)
            }

        }
        for (String data : dataCountMap.keySet()) {   //DATA = Fabio Gouw|250329
        	System.out.println("[DEBUG APP] data = " + data);
            int total = dataCountMap.get(data);      //TOTAL = 2
            System.out.println("[DEBUG APP] total = " + total);
            String[] userdata =  data.split("\\|");
            System.out.println("[DEBUG APP] userdata[0] = " + userdata[0]);
            System.out.println("[DEBUG APP] userdata[1] = " + userdata[1]);
            //userdata[0] = Fabio Gouw
            //userdata[1] = 250329
            String name = userdata[0];   //name = Fabio Gouw
            Integer id = Integer.parseInt(userdata[1]);   //	ERRO AQUI!!!
            map.put(new UserComparator(name, id, total), new Text(data + "\t" + total));
            if (map.size() > 10) {
                map.remove(map.firstKey());
            }
        }
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (Text t : map.values()) {
            context.write(NullWritable.get(), t);
        }

    }

}

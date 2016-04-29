package reducers;

import java.io.IOException;
import java.util.TreeMap;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import util.UserComparator;

public class TopTenBrazilianWritersReducer extends Reducer<NullWritable, Text, Text, Text> {
    private TreeMap<UserComparator, Text> map = new TreeMap<UserComparator, Text>(new UserComparator(null, null, null));
    private Text value = new Text();

    @Override
    protected void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        for (Text text : values) {
            String[] dataSplit = text.toString().split("\t");
            String[] userdata =  dataSplit[0].split("\\|");
            String name = userdata[0];
            Integer id = Integer.parseInt(userdata[1]);
            Integer reputation = Integer.parseInt(userdata[2]); 
            //value.set(name);
            value.set("Name: " + name +" |  Id: "   + id + " | Reputation: "  + reputation);
            map.put(new UserComparator(name, id, Integer.parseInt(dataSplit[1])), new Text(value));
            if (map.size() > 10) {
                map.remove(map.firstKey());
            }

        }

    }
    
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        for (UserComparator qtdePosts : map.descendingKeySet()) {
            value.set(qtdePosts.getCount().toString());
            context.write(map.get(qtdePosts), value);
        }
    }
}

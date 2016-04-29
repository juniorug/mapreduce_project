package reducers;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UserJoinReducer extends Reducer<Text, Text, Text, Text> {

    private ArrayList<Text> listA = new ArrayList<Text>();
    private ArrayList<Text> listB = new ArrayList<Text>();
        
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        
        listA.clear();
        listB.clear();
        
        for (Text tmp : values) {
            if (tmp.toString().charAt(0) == 'A') {
                listA.add(new Text(tmp.toString().substring(1)));
            } else if (tmp.toString().charAt(0) == 'B') {
                listB.add(new Text(tmp.toString().substring(1)));
            }

        }
        executeJoinLogic(context);
    }

    private void executeJoinLogic(Context context) throws IOException,
            InterruptedException {
        if (!listA.isEmpty() && !listB.isEmpty()) {
            for (Text A : listA) {
                for (Text B : listB) {
                    context.write(A, B);
                }
            }
        }
    }


}   
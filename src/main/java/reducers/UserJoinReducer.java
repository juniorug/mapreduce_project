package reducers;
/*
 * UserJoinReducer
 * 
 * 
 * @author Edivaldo Mascarenhas Jr.
 * @version 1.0
*/
import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * The Class UserJoinReducer.
 */
public class UserJoinReducer extends Reducer<Text, Text, Text, Text> {

    private static final Text EMPTY_TEXT = new Text("");
    private ArrayList<Text> listA = new ArrayList<Text>();
    private ArrayList<Text> listB = new ArrayList<Text>();
    private String joinType = null;
    
    public void setup(Context context) {
     // Get the type of join from our configuration 
        joinType = context.getConfiguration().get("join.type");
     }
    /* (non-Javadoc)
     * @see org.apache.hadoop.mapreduce.Reducer#reduce(KEYIN, java.lang.Iterable, org.apache.hadoop.mapreduce.Reducer.Context)
     */
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

    /**
     * Execute join logic.
     *
     * @param context the context
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws InterruptedException the interrupted exception
     */
    private void executeJoinLogic(Context context) throws IOException, InterruptedException {
        
        if (joinType.equalsIgnoreCase("inner")) {
            if (!listA.isEmpty() && !listB.isEmpty()) {
                for (Text A : listA) {
                    for (Text B : listB) {
                        context.write(A, B);
                    }
                }
            }
        } else if (joinType.equalsIgnoreCase("leftouter")) { // For each entry in A,
            for (Text A : listA) {
                // If list B is not empty, join A and B
                if (!listB.isEmpty()) 
                    { for (Text B : listB) {
                        context.write(A, B);
                    }
                }else{
                    // Else, output A by itself 
                    context.write(A, EMPTY_TEXT);
                } 
            }
        }
    }

}   
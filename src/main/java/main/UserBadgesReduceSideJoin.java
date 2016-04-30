package main;
/*
 * UserBadgesReduceSideJoin: Job que realiza o reduce side join 
 * entre Users e Badges (inner join)
 * 
 * 
 * @author Edivaldo Mascarenhas Jr.
 * @version 1.0
*/
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import mappers.BadgesMapper;
import mappers.UserMapperJoinMapper;
import reducers.UserJoinReducer;

/**
 * The Class UserBadgesReduceSideJoin.
 */
public class UserBadgesReduceSideJoin {
    
    /**
     * The main method.
     *
     * @param args the arguments
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ClassNotFoundException the class not found exception
     * @throws InterruptedException the interrupted exception
     */
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        if (otherArgs.length != 4) {
            System.err.println("Usage: UserBadgesReduceSideJoin <input_file1> <input_file2> <output_dir>");
            System.exit(2);
        }
        Path userPath = new Path(args[0]);
        Path BadgesPath = new Path(args[1]);
        Path outputDirpPath = new Path(args[3]);
        
        Job job = Job.getInstance(conf, "StackOverflow User Badges Reduce Side Join");

        job.setJarByClass(UserBadgesReduceSideJoin.class);
        MultipleInputs.addInputPath(job, userPath, TextInputFormat.class, UserMapperJoinMapper.class);
        MultipleInputs.addInputPath(job, BadgesPath, TextInputFormat.class, BadgesMapper.class);
        job.getConfiguration().set("join.type", args[2]);
        job.setReducerClass(UserJoinReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileOutputFormat.setOutputPath(job, outputDirpPath);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

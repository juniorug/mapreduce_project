package main;
/*
 * UserBadgesJoin: Job que realiza o replicated join 
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
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import mappers.ReplicatedJoinMapper;


/**
 * The Class UserBadgesJoin.
 */
public class UserBadgesReplicatedJoin {
    
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
            System.err.println("Usage: UserBadgesJoin <input_file1> <input_file2> <output_dir>");
            System.exit(2);
        }
        
        Job job = Job.getInstance(conf, "StackOverflow User Badges Replicated Join");
        job.getConfiguration().set("join.type", args[2]);
        //job.getConfiguration().set("join.type", "inner");

        job.setJarByClass(TopTenBrazilianWriters.class);
        job.setMapperClass(ReplicatedJoinMapper.class);
        job.setNumReduceTasks(0);

        TextInputFormat.setInputPaths(job, new Path(otherArgs[0]));  //datasetUserPath
        TextOutputFormat.setOutputPath(job, new Path(otherArgs[3]));  //outputDirpPath

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // Configure the DistributedCache
        job.addCacheFile(new Path(otherArgs[1]).toUri());  //datasetBadgesPath
        //DistributedCache.addCacheFile(new Path(otherArgs[1]).toUri(),job.getConfiguration());
        //DistributedCache.setLocalFiles(job.getConfiguration(), otherArgs[1]);

        
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}

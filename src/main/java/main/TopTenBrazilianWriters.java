package main;
/*
 * TopTenBrazilianWriters: Job que recupera o top ten
 * brasileiros com maior quantidade de posts
 * 
 * 
 * @author Edivaldo Mascarenhas Jr.
 * @version 1.0
*/
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import mappers.PostMapper;
import mappers.TopTenBrazilianWritersMapper;
import mappers.UserMapper;
import reducers.TopTenBrazilianWritersCombiner;
import reducers.TopTenBrazilianWritersReducer;
import reducers.UserJoinReducer;

/**
 * The Class TopTenBrazilianWriters.
 */
public class TopTenBrazilianWriters {
    
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

        if (otherArgs.length != 3) {
            System.err.println("Usage: TopTenBrazilianWriters <input_file1> <input_file2> <output_dir>");
            System.exit(2);
        }

        Path datasetUserPath = new Path(otherArgs[0]);
        Path datasetPostPath = new Path(otherArgs[1]);
        Path outputDirIntermediate = new Path(otherArgs[2] + "_int");
        Path outputDirpPath = new Path(otherArgs[2]);
        
        Job job = Job.getInstance(conf, "StackOverflow Top Ten Brazilian Post Writers P1");
        job.getConfiguration().set("mapRegex", "(.*)bra[sz]il(.*)");
        job.setJarByClass(TopTenBrazilianWriters.class);
        MultipleInputs.addInputPath(job, datasetUserPath, TextInputFormat.class, UserMapper.class);
        MultipleInputs.addInputPath(job, datasetPostPath, TextInputFormat.class, PostMapper.class);
        job.setReducerClass(UserJoinReducer.class);    
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileOutputFormat.setOutputPath(job, outputDirIntermediate);
        int code = job.waitForCompletion(true) ? 0 : 1;
        if (code == 0) {
            job = Job.getInstance(conf, "StackOverflow Top Ten Brazilian Post Writers P2");
            job.setOutputKeyClass(NullWritable.class);
            job.setOutputValueClass(Text.class);
            job.setJarByClass(TopTenBrazilianWriters.class);
            job.setMapperClass(TopTenBrazilianWritersMapper.class);
            job.setCombinerClass(TopTenBrazilianWritersCombiner.class);
            job.setReducerClass(TopTenBrazilianWritersReducer.class);
            job.setNumReduceTasks(1);
            FileInputFormat.addInputPath(job, outputDirIntermediate);
            FileOutputFormat.setOutputPath(job, outputDirpPath);
            code = job.waitForCompletion(true) ? 0 : 1;
        }
        
        FileSystem.get(conf).delete(outputDirIntermediate, true);
        System.exit(code);
    }

}

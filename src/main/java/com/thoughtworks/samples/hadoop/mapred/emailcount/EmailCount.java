package com.thoughtworks.samples.hadoop.mapred.emailcount;

import com.thoughtworks.samples.hadoop.mapred.generic.IdentityReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class EmailCount extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Configuration(), new EmailCount(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("emailcount.ignoredomain", "hotmail.com");
        Job emailCountJob = new Job(conf, "EmailCount");
        emailCountJob.setJarByClass(EmailCount.class);
        emailCountJob.setMapperClass(EmailMapper.class);
        emailCountJob.setReducerClass(IdentityReducer.class);
        emailCountJob.setOutputKeyClass(Text.class);
        emailCountJob.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(emailCountJob, new Path(args[0]));
        FileOutputFormat.setOutputPath(emailCountJob, new Path(args[1]));
        emailCountJob.setNumReduceTasks(2);
        emailCountJob.waitForCompletion(true);
        return 0;
    }
}

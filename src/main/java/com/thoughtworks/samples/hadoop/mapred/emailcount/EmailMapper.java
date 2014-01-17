package com.thoughtworks.samples.hadoop.mapred.emailcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EmailMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Pattern wordPattern
            = Pattern.compile("[a-z0-9.-]+\\.[a-z]{2,4}");

    @Override
    public void map(Object key, Text text, Context context)
            throws IOException, InterruptedException {
        String line = text.toString();
        String[] tokens = line.split("@");
        Configuration config =context.getConfiguration();
        String[] ignore = config.getStrings("emailcount.ignoredomain");
        System.out.println(ignore[0]);
        for (String token : tokens) {
            Matcher wordMatcher = wordPattern.matcher(token);
            if (wordMatcher.matches() && !token.matches(ignore[0])) {
                context.write(new Text(token), new IntWritable(1));
            }
        }
    }
}
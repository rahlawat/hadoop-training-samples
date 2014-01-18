package com.thoughtworks.samples.hadoop.mapred.emailcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class EmailMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Pattern wordPattern
            = Pattern.compile("[a-z0-9.-]+\\.[a-z]{2,4}");
    Map<Text, IntWritable> domainCounterMap;
    @Override
    public void setup(Context context){
       domainCounterMap = new HashMap<Text, IntWritable>();
    }

    @Override
    public void map(Object key, Text text, Context context)
            throws IOException, InterruptedException {
        String line = text.toString();
        String[] tokens = line.split("@");
        Configuration config =context.getConfiguration();
        String ignore = config.get("emailcount.ignoredomain");
        System.out.println(ignore);
        for (String token : tokens) {
            Matcher wordMatcher = wordPattern.matcher(token);
            if (wordMatcher.matches()) {
                String word = token;
                if(!word.matches(ignore)){
                    IntWritable count = domainCounterMap.get(token);
                    domainCounterMap.put(new Text(token), new IntWritable(count.get() + 1) );
                }
                else {
                    context.getCounter(IGNORED_DOMAINS.MATCHED).increment(1);
                }
            }
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        for (Text key : domainCounterMap.keySet()) {
            context.write(key, domainCounterMap.get(key));
        }
    }

}
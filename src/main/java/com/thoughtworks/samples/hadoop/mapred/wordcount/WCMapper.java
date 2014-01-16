ackage com.thoughtworks.samples.hadoop.mapred.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class WCMapper extends Mapper<Object, Text, Text, IntWritable> {

    private Pattern wordPattern
            = Pattern.compile("[a-z0-9.-]+\\.[a-z]{2,4}");

    @Override
    public void map(Object key, Text text, Context context)
            throws IOException, InterruptedException {
        String line = text.toString();
        String[] tokens = line.split("@");
        for (String token : tokens) {
            Matcher wordMatcher = wordPattern.matcher(token);
            if (wordMatcher.matches()) {
                context.write(new Text(token), new IntWritable(1));
            } else {
                context.getCounter(SKIPPED_WORDS.MISMATCHED_WORD).increment(1);
            }
        }
    }
}
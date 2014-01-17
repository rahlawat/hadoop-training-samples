//package com.thoughtworks.samples.hadoop.mapred.emailcount;
//
//import org.apache.hadoop.io.IntWritable;
//import org.apache.hadoop.io.LongWritable;
//import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mrunit.mapreduce.MapDriver;
//import org.junit.Test;
//
//import java.io.IOException;
//
//import static org.mockito.Mockito.*;
//
//public class EmailMapperTest {
//
//    @Test
//    public void should_count_each_domain_name_in_line_as_one_occurrence() throws IOException, InterruptedException {
//        EmailMapper emailMapper = new EmailMapper();
//        LongWritable dummyKey = new LongWritable();
//        Text inputValue = new Text("test@domain1.com test@domain2.com test@domain1.com");
//        Mapper.Context context = mock(Mapper.Context.class);
//        emailMapper.map(dummyKey, inputValue, context);
//
//        verify(context, times(2)).write(new Text("domain1.com"), new IntWritable(1));
//        verify(context, times(1)).write(new Text("domain2.com"), new IntWritable(1));
//    }
//
//
//    @Test
//    public void mapper_should_count_occurrence_of_each_domain() {
//
//        EmailMapper wcMapper = new EmailMapper();
//        MapDriver<Object, Text, Text, IntWritable> mapDriver
//                = MapDriver.newMapDriver(wcMapper);
//
//        mapDriver.withInput(new LongWritable(0), new Text("test@domain1.com test@domain2.com test@domain2.com test@domain1.com"));
//        mapDriver.withOutput(new Text("domain1.com"), new IntWritable(1));
//        mapDriver.withOutput(new Text("domain2.com"), new IntWritable(1));
//        mapDriver.withOutput(new Text("domain2.com"), new IntWritable(1));
//        mapDriver.withOutput(new Text("domain1.com"), new IntWritable(1));
//
//        mapDriver.runTest();
//    }

//    @Test
//    public void mapper_should_normalize_word_case() {
//
//        WCMapper wcMapper = new WCMapper();
//        MapDriver<Object, Text, Text, IntWritable> mapDriver
//                = MapDriver.newMapDriver(wcMapper);
//
//        mapDriver.withInput(new LongWritable(0), new Text("Word1 word1"));
//        mapDriver.withOutput(new Text("word1"), new IntWritable(1));
//        mapDriver.withOutput(new Text("word1"), new IntWritable(1));
//
//        mapDriver.runTest();
//    }
//
//    @Test
//    public void mapper_should_ignore_punctuation() {
//
//        WCMapper wcMapper = new WCMapper();
//        MapDriver<Object, Text, Text, IntWritable> mapDriver
//                = MapDriver.newMapDriver(wcMapper);
//
//        mapDriver.withInput(new LongWritable(0), new Text("word1. word2, -word3"));
//        mapDriver.withOutput(new Text("word1"), new IntWritable(1));
//        mapDriver.withOutput(new Text("word2"), new IntWritable(1));
//        mapDriver.withOutput(new Text("word3"), new IntWritable(1));
//
//        mapDriver.runTest();
//    }
//}

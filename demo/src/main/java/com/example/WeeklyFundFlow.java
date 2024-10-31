package com.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

public class WeeklyFundFlow {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private Text weekday = new Text();
        private Text flow = new Text();
        private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd", Locale.ENGLISH);
        private SimpleDateFormat dayFormat = new SimpleDateFormat("EEEE", Locale.ENGLISH);

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length < 2) return; // 跳过无效行

            String dateStr = fields[0];
            String[] amounts = fields[1].split(",");
            if (amounts.length < 2) return; // 跳过无效行

            try {
                Date date = dateFormat.parse(dateStr);
                String dayOfWeek = dayFormat.format(date);
                weekday.set(dayOfWeek);
                flow.set(amounts[0] + "," + amounts[1]);
                context.write(weekday, flow);
            } catch (ParseException e) {
                e.printStackTrace();
            }
        }
    }

    public static class SumReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long totalPurchase = 0;
            long totalRedeem = 0;
            int count = 0;

            for (Text val : values) {
                String[] amounts = val.toString().split(",");
                totalPurchase += Long.parseLong(amounts[0]);
                totalRedeem += Long.parseLong(amounts[1]);
                count++;
            }

            long avgPurchase = totalPurchase / count;
            long avgRedeem = totalRedeem / count;

            result.set(avgPurchase + "," + avgRedeem);
            context.write(key, result);
        }
    }

    public static class SortMapper extends Mapper<Object, Text, LongWritable, Text> {

        private LongWritable purchaseAmount = new LongWritable();
        private Text weekday = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split("\t");
            if (fields.length < 2) return; // 跳过无效行

            String dayOfWeek = fields[0];
            String[] amounts = fields[1].split(",");
            if (amounts.length < 2) return; // 跳过无效行

            long purchase = Long.parseLong(amounts[0]);
            purchaseAmount.set(purchase);
            weekday.set(dayOfWeek + "\t" + amounts[0] + "," + amounts[1]);
            context.write(purchaseAmount, weekday);
        }
    }

    public static class SortReducer extends Reducer<LongWritable, Text, Text, Text> {

        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(null, val);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();

        // Job 1: Calculate average fund flow per weekday
        Job job1 = Job.getInstance(conf, "weekly fund flow");
        job1.setJarByClass(WeeklyFundFlow.class);
        job1.setMapperClass(TokenizerMapper.class);
        job1.setReducerClass(SumReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(args[1]));
        job1.waitForCompletion(true);

        // Job 2: Sort by average purchase amount
        Job job2 = Job.getInstance(conf, "sort by purchase amount");
        job2.setJarByClass(WeeklyFundFlow.class);
        job2.setMapperClass(SortMapper.class);
        job2.setReducerClass(SortReducer.class);
        job2.setOutputKeyClass(LongWritable.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(args[1]));
        FileOutputFormat.setOutputPath(job2, new Path(args[2]));
        job2.setSortComparatorClass(LongWritable.DecreasingComparator.class);
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}
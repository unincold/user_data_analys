package com.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class DailyFundFlow {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private Text date = new Text();
        private Text flow = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length < 14) return; // 跳过无效行

            String reportDate = fields[1];
            String totalPurchaseAmt = fields[4].isEmpty() ? "0" : fields[4];
            String totalRedeemAmt = fields[10].isEmpty() ? "0" : fields[10];

            date.set(reportDate);
            flow.set(totalPurchaseAmt + "," + totalRedeemAmt);
            context.write(date, flow);
        }
    }

    public static class SumReducer extends Reducer<Text, Text, Text, Text> {

        private Text result = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            long totalPurchase = 0;
            long totalRedeem = 0;

            for (Text val : values) {
                String[] amounts = val.toString().split(",");
                totalPurchase += Long.parseLong(amounts[0]);
                totalRedeem += Long.parseLong(amounts[1]);
            }

            result.set(totalPurchase + "," + totalRedeem);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "daily fund flow");
        job.setJarByClass(DailyFundFlow.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(SumReducer.class);
        job.setReducerClass(SumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
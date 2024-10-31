package com.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class InterestRateImpact {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private Text interestRateRange = new Text();
        private Text flow = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length < 14) return; // 跳过无效行

            String reportDate = fields[1];
            String totalPurchaseAmt = fields[4].isEmpty() ? "0" : fields[4];
            String totalRedeemAmt = fields[10].isEmpty() ? "0" : fields[10];

            // 假设我们有一个方法getInterestRateRange来获取利率区间
            String interestRate = getInterestRateRange(reportDate);
            interestRateRange.set(interestRate);
            flow.set(totalPurchaseAmt + "," + totalRedeemAmt);
            context.write(interestRateRange, flow);
        }

        private String getInterestRateRange(String date) {
            // 这里应该实现根据日期获取利率区间的逻辑
            // 例如：根据日期查询mfd_bank_shibor表并返回对应的利率区间
            return "2.5-3.0%"; // 示例返回值
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

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "interest rate impact");
        job.setJarByClass(InterestRateImpact.class);
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
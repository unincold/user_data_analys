package com.example;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class InterestRateImpact {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {

        private Text interestRateRange = new Text();
        private Text flow = new Text();
        private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd", Locale.ENGLISH);
        private Map<String, Double> interestRates = new HashMap<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            Path path = new Path(conf.get("interest.rate.path"));
            FileSystem fs = FileSystem.get(conf);
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));
            String line;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                if (fields.length < 2) continue;
                interestRates.put(fields[0], Double.parseDouble(fields[1])); // Assuming the date is in the first column and the interest rate is in the second column
            }
            br.close();
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] fields = value.toString().split(",");
            if (fields.length < 14) return; // 跳过无效行

            String reportDate = fields[1];
            String totalPurchaseAmt = fields[4].isEmpty() ? "0" : fields[4];
            String totalRedeemAmt = fields[10].isEmpty() ? "0" : fields[10];

            String interestRate = getInterestRateRange(reportDate);
            interestRateRange.set(interestRate);
            flow.set(totalPurchaseAmt + "," + totalRedeemAmt);
            context.write(interestRateRange, flow);
        }

        private String getInterestRateRange(String date) {
            Double rate = interestRates.get(date);
            if (rate == null) return "Unknown";
            if (rate < 3.0) return "0-3.0%";
            else if (rate < 4.0) return "3.0-4.0%";
            else if (rate < 5.0) return "4.0-5.0%";
            else return "5.0%+";
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
        conf.set("interest.rate.path", args[2]); // Path to the interest rate file
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
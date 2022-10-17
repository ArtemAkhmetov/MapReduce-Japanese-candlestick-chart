import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import java.io.IOException;
import java.util.StringTokenizer;
import java.util.Comparator;
//import java.util.Date;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.Collections;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import java.util.regex.Pattern;
import java.math.BigDecimal;

public class candle {

    public static class candleMapper
            extends Mapper<LongWritable, Text, Text, PriceMoment>{

        private Text word = new Text();
        private Text symbol = new Text();
        private Text moment = new Text();
        private Text ID_DEAL = new Text();
        private Text price = new Text();
        private Text symbol_moment = new Text();
        private PriceMoment price_moment = new PriceMoment();

        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {
            if (key.get() == 0 && value.toString().contains("#SYMBOL")) {
                return;
            }
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            int i = 0;
            int hourOfDay,minute,second,ms;
            Configuration conf = context.getConfiguration();
            int width = conf.getInt("candle.width", 300000);
            String securities = conf.get("candle.securities",".*");
            long date_from = conf.getLong("candle.date.from", 19000101);
            long date_to = conf.getLong("candle.date.to", 20200101);
            int time_from = conf.getInt("candle.time.from", 1000);
            int time_to = conf.getInt("candle.time.to", 1800);
            while (itr.hasMoreTokens() && i < 8) {
                word.set(itr.nextToken());
                if (i == 0) {
                    if (Pattern.matches(securities, word.toString())) {
                        symbol.set(word);
                    }
                    else {
                        return;
                    }
                }
                else if (i == 2) {
                    moment.set(word);
                }
                else if (i == 3) {
                    ID_DEAL.set(word);
                }
                else if (i == 4) {
                    price.set(word);
                }
                ++i;
            }

            long date;
            date = Long.parseLong(moment.toString().substring(0,8));

            hourOfDay = Integer.parseInt(moment.toString().substring(8,10));
            minute = Integer.parseInt(moment.toString().substring(10,12));
            second = Integer.parseInt(moment.toString().substring(12,14));
            ms = Integer.parseInt(moment.toString().substring(14,17));

            int tmp1;
            tmp1 = hourOfDay * 60 * 60 * 1000 + minute * 60 * 1000 + second * 1000 + ms;
            int newHour, newMinute, newSecond, newMS;

            int time_from_hour, time_from_minute, time_to_hour, time_to_minute;
            time_from_hour = time_from / 100;
            time_from_minute = time_from % 100;
            time_to_hour = time_to / 100;
            time_to_minute = time_to % 100;

            int time_from_MS, time_to_MS;
            time_from_MS = time_from_hour * 60 * 60 * 1000 + time_from_minute * 60 * 1000;
            time_to_MS = time_to_hour * 60 * 60 * 1000 + time_to_minute * 60 * 1000;

            int num_candles;
            num_candles = (time_to_MS - time_from_MS) / width;

            if (date_from <= date && date < date_to && tmp1 >= time_from_MS && tmp1 < time_from_MS + num_candles * width) {
                int id_candle = (tmp1 - time_from_MS) / width;
                long candle_moment_MS = time_from_MS + id_candle * width;
                newHour = (int) (candle_moment_MS / (60*60*1000));
                candle_moment_MS -= newHour * 60 * 60 * 1000;
                newMinute = (int) (candle_moment_MS / (60*1000));
                candle_moment_MS -= newMinute * 60 * 1000;
                newSecond = (int) (candle_moment_MS / 1000);
                candle_moment_MS -= newSecond * 1000;
                newMS = (int) candle_moment_MS;

                price_moment.set(Double.parseDouble(price.toString()), Long.parseLong(moment.toString()), Long.parseLong(ID_DEAL.toString()), id_candle);

                String tmpStr = String.valueOf(date) + String.format("%02d%02d%02d%03d", newHour, newMinute, newSecond, newMS);
                symbol_moment.set(symbol.toString() + "-" + tmpStr);
                context.write(symbol_moment, price_moment);
            }
        }
    }

    public static class candlePartitioner
            extends Partitioner<Text, PriceMoment> {
        @Override
        public int getPartition(Text key, PriceMoment value, int numPartitions) {
            int id_candle = value.getId_candle();
            return id_candle % numPartitions;
        }
    }

    public static class candleReducer
            extends Reducer<Text,PriceMoment,Text,Text> {

        MultipleOutputs multipleOutputs;

        @Override
        protected void setup(Context context) throws IOException,
                InterruptedException {
            multipleOutputs = new MultipleOutputs(context);
        }

        private Text symbol = new Text();
        private Text result = new Text();

        public void reduce(Text key, Iterable<PriceMoment> prices_moments,
                           Context context
        ) throws IOException, InterruptedException {
            double open=0.0f, high=-Double.MAX_VALUE, low=Double.MAX_VALUE, close=0.0f, roundedPrice;
            long openMoment=Long.MAX_VALUE, closeMoment=0, openID_DEAL=Long.MAX_VALUE, closeID_DEAL=0;
            String[] splitted_key = key.toString().split("-");
            List<PriceMoment> buffer = new ArrayList<PriceMoment>();
            for (PriceMoment price_moment : prices_moments) {
                buffer.add(new PriceMoment(price_moment));
            }

            Collections.sort(buffer, new Comparator<PriceMoment>() {
                public int compare(PriceMoment o1, PriceMoment o2) {
                    return o1.getMoment().compareTo(o2.getMoment());
                }
            });

            for (PriceMoment price_moment : buffer) {
                roundedPrice = BigDecimal.valueOf(price_moment.getPrice()).setScale(1, BigDecimal.ROUND_HALF_UP).doubleValue();
                if (openMoment == price_moment.getMoment() && openID_DEAL > price_moment.getID_DEAL() || openMoment > price_moment.getMoment()) {
                    open = roundedPrice;
                    openMoment = price_moment.getMoment();
                    openID_DEAL = price_moment.getID_DEAL();
                }
                if (closeMoment == price_moment.getMoment() && closeID_DEAL < price_moment.getID_DEAL() || closeMoment < price_moment.getMoment()) {
                    close = roundedPrice;
                    closeMoment = price_moment.getMoment();
                    closeID_DEAL = price_moment.getID_DEAL();
                }
                if (high < roundedPrice) {
                    high = roundedPrice;
                }
                if (low > roundedPrice) {
                    low = roundedPrice;
                }
            }
            result.set(splitted_key[0] + "," + splitted_key[1] + "," + String.format(Locale.US, "%.1f", open) +
                    "," + String.format(Locale.US, "%.1f", high) + "," + String.format(Locale.US, "%.1f", low) + "," +
                    String.format(Locale.US, "%.1f", close));
            symbol.set(splitted_key[0]);
            NullWritable nullWritable = NullWritable.get();
            multipleOutputs.write(nullWritable, result, symbol.toString());
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            multipleOutputs.close();
        }
    }

    public static void main(String[] args) throws Exception {
        //long start = new Date().getTime();
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        GenericOptionsParser ops = new GenericOptionsParser(conf, args);
        conf = ops.getConfiguration();
        int num_reducers = conf.getInt("candle.num.reducers", 1);
        if (otherArgs.length != 2) {
            System.err.println("Usage: candle <in> <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf);
        job.setJobName("candle");
        job.setJarByClass(candle.class);
        job.setMapperClass(candleMapper.class);
        job.setPartitionerClass(candlePartitioner.class);
        job.setReducerClass(candleReducer.class);
        job.setNumReduceTasks(num_reducers);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PriceMoment.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

        /*boolean status = job.waitForCompletion(true);
        long end = new Date().getTime();
        System.out.println("Job took "+(end-start) + "milliseconds");
        System.exit(0);*/
    }
}
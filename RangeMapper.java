import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RangeMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static final int MISSING = 99999;

    @Override
    public void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
        
        String line = value.toString();
        String month = line.substring(19.21);
        int skyCeilingHeight = Integer.parseInt(line.substring(70, 75));
        String quality = line.substring(75,76);
        if (skyCeilingHeight != MISSING && quality.matches("[01459]")) {
            context.write(new Text(month), new IntWritable(skyCeilingHeight));
        }
    }
}

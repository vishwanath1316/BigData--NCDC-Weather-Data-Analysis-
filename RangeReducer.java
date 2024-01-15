import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class RangeReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int max = Integer.MIN_VALUE;
        int min = Integer.MAX_VALUE;
        for (IntWritable value : values) {
            int height = value.get();
            if (height > max) {
                max = height;
            }
            if (height < min) {
                min = height;
            }
        }
        int range = max - min;
        context.write(key, new IntWritable(range));
    }
}

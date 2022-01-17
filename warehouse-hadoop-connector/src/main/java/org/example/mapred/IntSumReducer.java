package org.example.mapred;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    private IntWritable result = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        IntWritable writable;
        for (Iterator iterator = values.iterator(); iterator.hasNext(); sum += writable.get()) {
            writable = (IntWritable) iterator.next();
        }
        this.result.set(sum);
        context.write(key, this.result);
    }
}

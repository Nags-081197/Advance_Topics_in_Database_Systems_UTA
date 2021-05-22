import java.io.IOException;
import java.util.Scanner;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.Vector;
import org.apache.hadoop.conf.Configuration;


class Elem implements Writable {
    public int tag;
    public int index;
    public double value;

    Elem () {}

    Elem(int tag, int index, double value) {
        this.tag = tag;
        this.index = index;
        this.value = value;
    }


    public void write ( DataOutput out ) throws IOException {
        out.writeInt(tag);
        out.writeInt(index);
        out.writeDouble(value);
    }


    public void readFields ( DataInput in ) throws IOException {
        tag = in.readInt();
        index = in.readInt();
        value = in.readDouble();
    }


}

class Pair implements WritableComparable<Pair> {
    public int i;
    public int j;

    Pair () {}

    Pair(int i, int j) {
        this.i = i;
        this.j = j;
    }


    public void write(DataOutput out) throws IOException {
        out.writeInt(i);
        out.writeInt(j);
    }


    public void readFields(DataInput in) throws IOException {
        i = in.readInt();
        j = in.readInt();
    }


    public int compareTo(Pair p) {
        if (i > p.i) {
            return 1;
        } 
        else if (i < p.i) {
            return -1;
        } 
        else {
            if (j > p.j) {
                return 1;
            } 
            else if (j < p.j) {
                return -1;
            } 
            else {
                return 0;
            }
        }
    }


    public String toString() {
        return i + " " + j + " ";
    }


}

public class Multiply {

    public static class MapperforM extends Mapper<Object,Text,IntWritable,Elem> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int i = s.nextInt();
            int j = s.nextInt();
            double v = s.nextDouble();
            context.write(new IntWritable(j),new Elem(0, i, v));
            s.close();
        }
    }


    public static class MapperforN extends Mapper<Object,Text,IntWritable,Elem> {
        @Override
        public void map ( Object key, Text value, Context context )
                        throws IOException, InterruptedException {
            Scanner s = new Scanner(value.toString()).useDelimiter(",");
            int i = s.nextInt();
            int j = s.nextInt();
            double v = s.nextDouble();
            context.write(new IntWritable(i),new Elem(1, j, v));
            s.close();
        }
    }

    public static class MapperforPair extends Mapper<Pair,DoubleWritable,Pair,DoubleWritable> {
        @Override
        public void map ( Pair key, DoubleWritable value, Context context )
                        throws IOException, InterruptedException {

            context.write(key, value);
        }
    }

    public static class ReducerMultiply extends Reducer<IntWritable,Elem,Pair,DoubleWritable> {
        @Override
        public void reduce ( IntWritable key, Iterable<Elem> values, Context context )
                           throws IOException, InterruptedException {
            ArrayList<Elem> A = new ArrayList<Elem>();
            ArrayList<Elem> B = new ArrayList<Elem>();
            A.clear();
            B.clear();

            Configuration conf = context.getConfiguration();
            for (Elem v: values) {
                Elem Temp = ReflectionUtils.newInstance(Elem.class, conf);
                ReflectionUtils.copy(conf, v, Temp);
                if (Temp.tag == 0) {
                    A.add(Temp);
                } else {
                    B.add(Temp);
                }
            }
                
            for ( Elem a: A ) {
                for ( Elem b: B ) {
                    context.write(new Pair(a.index,b.index),new DoubleWritable(a.value*b.value));
                }
            }

        }
    }

    public static class ReducerSummation extends Reducer<Pair,DoubleWritable,Pair,DoubleWritable> {
        @Override
        public void reduce ( Pair key, Iterable<DoubleWritable> values, Context context )
                           throws IOException, InterruptedException {
            Double op = 0.0;
            for (DoubleWritable v: values) {
                op = op+v.get();
            }    
            context.write(key,new DoubleWritable(op));            
        }
    }


    public static void main ( String[] args ) throws Exception {

        Job job1 = Job.getInstance();
        job1.setJobName("Job1");
        job1.setJarByClass(Multiply.class);
        job1.setOutputKeyClass(Pair.class);
        job1.setOutputValueClass(DoubleWritable.class);
        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Elem.class);
        job1.setReducerClass(ReducerMultiply.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        MultipleInputs.addInputPath(job1,new Path(args[0]),TextInputFormat.class,MapperforM.class);
        MultipleInputs.addInputPath(job1,new Path(args[1]),TextInputFormat.class,MapperforN.class);
        FileOutputFormat.setOutputPath(job1,new Path(args[2]));
        job1.waitForCompletion(true);

        Job job2 = Job.getInstance();
        job2.setJobName("Job2");
        job2.setJarByClass(Multiply.class);
        job2.setOutputKeyClass(Pair.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setMapOutputKeyClass(Pair.class);
        job2.setMapOutputValueClass(DoubleWritable.class);
        job2.setMapperClass(MapperforPair.class);
        job2.setReducerClass(ReducerSummation.class);
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.setInputPaths(job2,new Path(args[2]));
        FileOutputFormat.setOutputPath(job2,new Path(args[3]));
        job2.waitForCompletion(true);

    }
}
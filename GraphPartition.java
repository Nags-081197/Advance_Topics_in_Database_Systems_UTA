import java.io.*;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
    public long id;                   // the vertex ID
    public Vector<Long> adjacent;     // the vertex neighbors
    public long centroid;             // the id of the centroid in which this vertex belongs to
    public short depth;               // the BFS depth
    

    Vertex() {}


    Vertex(long id, Vector<Long> adjacent, long centroid, short depth) {

		this.id = id;
		this.adjacent = adjacent;
		this.centroid = centroid;
		this.depth = depth;
    }

    
    public void write (DataOutput out) throws IOException {
		out.writeLong(id);
		LongWritable size  = new LongWritable(adjacent.size());
		size.write(out);
		for( Long adjacentVector : adjacent) {
			out.writeLong(adjacentVector);
        }
        out.writeLong(centroid);
		out.writeShort(depth);
    }
    
    public void readFields(DataInput in) throws IOException {

        id = in.readLong();
        adjacent = new Vector<Long>();
        adjacent.clear();
		LongWritable size = new LongWritable();
		size.readFields(in);
		for (int i=0;i<size.get();i++) {
			LongWritable adjacentVector=new LongWritable();
			adjacentVector.readFields(in);
			adjacent.add(adjacentVector.get());

        }
        centroid = in.readLong();
		depth = in.readShort();
    }


}

public class GraphPartition {
    static Vector<Long> centroids = new Vector<Long>();
    final static short max_depth = 8;
    static short BFS_depth = 0;
    static int cnt = 0;

    public static class MyMapper1 extends Mapper<Object,Text,LongWritable,Vertex>{
    	@Override
    	protected void map(Object key, Text value, Context context)
    			throws IOException, InterruptedException {
            String linevalue = value.toString();
            String[] longParse = linevalue.split(",");
            long id = Long.parseLong(longParse[0]);
            long centroid = 0;
            Vector<Long> adjacent = new Vector<Long>();

            for(int i=1;i<longParse.length;i++) {
                adjacent.add(Long.parseLong(longParse[i]));
            }

            if (cnt < 10) 
            {
                centroid = id;
                cnt++;
            }
            else 
            {
                centroid = -1;
                cnt++;
            }
        context.write(new LongWritable(id), new Vertex(id,adjacent,centroid,(short)0));
    	}

    }

    public static class MyMapper2 extends Mapper<LongWritable,Vertex,LongWritable,Vertex>{

    	public  void map(LongWritable key,Vertex vertex,Context context ) throws IOException, InterruptedException {
    		context.write(new LongWritable(vertex.id), vertex);
            if(vertex.centroid>0) {
                    for(long n : vertex.adjacent) {
                        context.write(new LongWritable(n),new Vertex(n, new Vector<Long>(), vertex.centroid, BFS_depth));
                    }
                }
    	}
    }

    public static class MyReducer2 extends Reducer<LongWritable,Vertex,LongWritable,Vertex>{
    	public void reduce(LongWritable key, Iterable<Vertex> values, Context context) throws IOException, InterruptedException {
            
            short min_depth = 1000;
            Vertex m = new Vertex(key.get(), new Vector<Long>(), (long)-1, (short)0);
            
    		for(Vertex v : values) {
    			if(!(v.adjacent.isEmpty())) {
    				m.adjacent = v.adjacent;
	            }
    			if (v.centroid > 0 && v.depth < min_depth) {
    				min_depth = v.depth;
    				m.centroid = v.centroid;
    			}
            }
            m.depth = min_depth;
    		context.write(key, m);
    	}
    }

    public static class MyMapper3 extends Mapper<LongWritable,Vertex,LongWritable,LongWritable>{
        @Override
        public void map(LongWritable key,Vertex value,Context context) throws IOException, InterruptedException {
                context.write(new LongWritable(value.centroid),new LongWritable(1));
            }
        }

    public static class MyReducer3 extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable>{
        protected void reduce(LongWritable centroid, Iterable<LongWritable> values, Context context ) throws IOException, InterruptedException {

            long m = 0;
            for (LongWritable v: values) {
                m = m+v.get();
            }
            context.write(centroid, new LongWritable(m));
        }
    }


    public static void main ( String[] args ) throws Exception {
        
        Job job1 = Job.getInstance();
        job1.setJobName("MyJob1");
        job1.setJarByClass(GraphPartition.class);
        job1.setMapperClass(MyMapper1.class);
        job1.setOutputKeyClass(LongWritable.class);
        job1.setOutputValueClass(Vertex.class);
        job1.setMapOutputKeyClass(LongWritable.class);
        job1.setMapOutputValueClass(Vertex.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
        job1.setInputFormatClass(TextInputFormat.class);
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path( args[1]+"/i0"));

        job1.waitForCompletion(true);
        for ( short i = 0; i < max_depth; i++ ) {
            BFS_depth++;
            Job job2 = Job.getInstance();
            job2.setJarByClass(GraphPartition.class);
            job2.setMapperClass(MyMapper2.class);
            job2.setReducerClass(MyReducer2.class);
            job2.setOutputKeyClass(LongWritable.class);
            job2.setOutputValueClass(Vertex.class);
            job2.setMapOutputKeyClass(LongWritable.class);
            job2.setMapOutputValueClass(Vertex.class);
            job2.setOutputFormatClass(SequenceFileOutputFormat.class);
            job2.setInputFormatClass(SequenceFileInputFormat.class);
            FileInputFormat.setInputPaths(job2, new Path(args[1]+"/i"+i));
            FileOutputFormat.setOutputPath(job2, new Path(args[1]+"/i"+(i+1)));
            job2.waitForCompletion(true);
        }

        Job job3 = Job.getInstance();
        job3.setJarByClass(GraphPartition.class);
        job3.setMapperClass(MyMapper3.class);
        job3.setReducerClass(MyReducer3.class);
        job3.setOutputKeyClass(LongWritable.class);
        job3.setOutputValueClass(LongWritable.class);
        job3.setMapOutputKeyClass(LongWritable.class);
        job3.setMapOutputValueClass(LongWritable.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        FileInputFormat.setInputPaths(job3, new Path(args[1]+"/i8"));
        FileOutputFormat.setOutputPath(job3, new Path(args[2]));
        job3.waitForCompletion(true);
    }
}

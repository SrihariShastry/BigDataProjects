import java.io.*;
import java.net.URI;
import java.util.Scanner;
import java.util.Vector;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;


class Vertex implements Writable {
	public long id;                   // the vertex ID
	public Vector<Long> adjacent;     // the vertex neighbors
	public long centroid;             // the id of the centroid in which this vertex belongs to
	public short depth;               // the BFS depth
	/* Default Constructor */
	Vertex(){}
	/* Constructor with arguments */
	Vertex ( long id,Vector<Long> adjacent,long centroid,short depth ) {
		this.id = id ; 
		this.centroid = centroid;
		this.depth = depth; 
		this.adjacent = adjacent; 
	}
	/* write to Out*/
	public void write(DataOutput out) throws IOException {
		out.writeLong(id);
		out.writeLong(centroid);
		out.writeShort(depth);
		out.writeLong(adjacent.size());
		int i =0;
		while(i<adjacent.size()) {
			out.writeLong(adjacent.get(i));
			i++;
		}	
	}
	/*Read from in*/
	public void readFields(DataInput in) throws IOException {
		this.id = in.readLong();
		this.centroid = in.readLong();
		this.depth = in.readShort();
		long s=in.readLong();
		adjacent=new Vector<Long>();
		int i =0;
		while(i<s) {
			this.adjacent.addElement(in.readLong());
			i++;
		}
	}
}

public class GraphPartition {
	static Vector<Long> centroids = new Vector<Long>();
	final static short max_depth = 8;
	static short BFS_depth = 0;

	/* First Mapper class*/
	public static class InitMapper extends Mapper<Object,Text,LongWritable,Vertex>{
		int count=0;
		@Override
		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

			long centroid;
			String vertex = value.toString();				// parse each line of Input file
			String[] vertices = vertex.split(",");			// split each line by comma
			// Assign the first 10 nodes as centroids.
			if(count<10) {
				centroid= Long.parseLong(vertices[0]);
			}else {
				centroid = -1;
			}
			count++;
			
			//loop counter
			int i = 1;
			Vector<Long> adjacent = new Vector<Long>();
			// Get the adjacent nodes
			while(i<vertices.length) {
				adjacent.add(Long.parseLong(vertices[i]));
				i++;
			}
			
			Vertex firstCentroid = new Vertex(Long.parseLong(vertices[0]),adjacent,centroid,(short)0);
			context.write(new LongWritable(Long.parseLong(vertices[0])),firstCentroid);  //key, Value
		}
	}
	/* Second Stage Mapper class */
	public static class IntermediateMapper extends Mapper<LongWritable, Vertex, LongWritable, Vertex> {

		@Override
		protected void map(LongWritable key, Vertex value,
				Mapper<LongWritable, Vertex, LongWritable, Vertex>.Context context)
						throws IOException, InterruptedException {
			//Send Graph Topology
			context.write(new LongWritable(value.id), value);
			//if vertex a centroid or if vertex has already been assigned a centroid
			Vector<Long> emptyAdjecents = new Vector<Long>();
			if(value.centroid>0) {
				for(long n:value.adjacent) {
					context.write(new LongWritable(n),new Vertex(n, emptyAdjecents, value.centroid, BFS_depth));
				}
			}
		}
	}
	
	/* Second Stage Reducer Class*/
	public static class IntermediateReducer extends Reducer<LongWritable, Vertex, LongWritable, Vertex>{

		@Override
		protected void reduce(LongWritable key, Iterable<Vertex> value,
				Reducer<LongWritable, Vertex, LongWritable, Vertex>.Context context)
						throws IOException, InterruptedException {
			short min_depth = 1000;
			Vertex m = new Vertex(key.get(), new Vector<Long>(),-1,(short)0);
			for(Vertex v: value) {
				if(!v.adjacent.isEmpty()) {
					m.adjacent=v.adjacent;
				}
				if(v.centroid>0 && v.depth<min_depth) {
					min_depth=v.depth;
					m.centroid = v.centroid;
				}	
			}
			m.depth = min_depth;
			context.write(key,m);
		}

	}
	public static class FinalMapper extends Mapper<LongWritable, Vertex, LongWritable, LongWritable>{

		@Override
		protected void map(LongWritable key, Vertex value,
				Mapper<LongWritable, Vertex, LongWritable, LongWritable>.Context context)
						throws IOException, InterruptedException {
			//send the centroids
			context.write(new LongWritable(value.centroid), new LongWritable(1));
		}
	}
	public static class FinalReducer extends Reducer<LongWritable, LongWritable, LongWritable, LongWritable>{

		@Override
		protected void reduce(LongWritable key, Iterable<LongWritable> values,
				Reducer<LongWritable, LongWritable, LongWritable, LongWritable>.Context context)
						throws IOException, InterruptedException {
			//count the  points with each centroids
			long m = 0;
			for(LongWritable v:values) {
				m=m+v.get();
			}
			context.write(new LongWritable(key.get()), new LongWritable(m));
		}		
	}


	public static void main ( String[] args ) throws Exception {
		/* ... First Map-Reduce job to read the graph */
		Job job = Job.getInstance();
		job.setJobName("Graph");
		job.setJarByClass(GraphPartition.class);
		
		job.setOutputKeyClass(LongWritable.class);					//reducer output key type
		job.setOutputValueClass(Vertex.class);						// reducer output Value type
		
		job.setMapOutputKeyClass(LongWritable.class);				//mapper output key type
		job.setMapOutputValueClass(Vertex.class);					//reducer output value type
		
		job.setMapperClass(InitMapper.class);
		
		job.setInputFormatClass(TextInputFormat.class);				//File is the input hence TextInputFormat
		job.setOutputFormatClass(SequenceFileOutputFormat.class);	

		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]+"/i0"));		// output will be the input for the next job			
		job.waitForCompletion(true);

		/* ... Second Map-Reduce job to do BFS */
		for ( short i = 0; i < max_depth; i++ ) {
			BFS_depth++;
			Job job1 = Job.getInstance();
			job1.setJobName("TopologyMapping");
			job1.setJarByClass(GraphPartition.class);
			
			job1.setOutputKeyClass(LongWritable.class); 					//reducer output key type
			job1.setOutputValueClass(Vertex.class);						//reducer output value type
			
			job1.setMapOutputKeyClass(LongWritable.class);				//mapper output key type
			job1.setMapOutputValueClass(Vertex.class);					//mapper output value type
			
			job1.setMapperClass(IntermediateMapper.class);
    		job1.setReducerClass(IntermediateReducer.class);
    		
			job1.setInputFormatClass(SequenceFileInputFormat.class);
			job1.setOutputFormatClass(SequenceFileOutputFormat.class);

    		FileInputFormat.setInputPaths(job1, new Path(args[1]+"/i"+i));
			FileOutputFormat.setOutputPath(job1, new Path(args[1]+"/i"+(i+1)));		

			job1.waitForCompletion(true);
		}
		Job job2 = Job.getInstance();
		/* ... Final Map-Reduce job to calculate the cluster sizes */
		job2.setJobName("ClusterSizeCalculation");
		job2.setJarByClass(GraphPartition.class);
		
		job2.setOutputKeyClass(LongWritable.class);
		job2.setOutputValueClass(LongWritable.class);
		
		job2.setMapOutputKeyClass(LongWritable.class);
		job2.setMapOutputValueClass(LongWritable.class);
		
		job2.setMapperClass(FinalMapper.class);
		job2.setReducerClass(FinalReducer.class);
		
		job2.setInputFormatClass(SequenceFileInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job2, new Path(args[1]+"/i8"));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.waitForCompletion(true);
	}
}

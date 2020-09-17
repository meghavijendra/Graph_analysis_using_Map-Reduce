import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Vector;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


class Vertex implements Writable {
	public short tag;                 // 0 for a graph vertex, 1 for a group number
	public long group;                // the group where this vertex belongs to
	public long vid;                  // the vertex ID
	public Vector<Long> adjacent;     // the vertex neighbors

	public Vertex() {
		this.tag =0;
		this.adjacent = new Vector<Long>();
	}
	public Vertex(short tag,long group,long VID,Vector<Long> adjacent) {
		this.tag = tag;
		this.group = group;
		this.vid = VID;
		this.adjacent = adjacent;
	}
	public Vertex(short tag,long group)
	{
		this.tag = tag;
		this.group = group;
		this.adjacent = new Vector<Long>();
	}
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeShort(tag);
		out.writeLong(group);
		out.writeLong(vid);
		out.writeInt(adjacent.size());
		for(int i=0; i<adjacent.size();i++)
		{
			out.writeLong(adjacent.get(i));
		}
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		tag= in.readShort();
		group= in.readLong();
		vid= in.readLong();
		adjacent = new Vector<Long>();
		int size = in.readInt();
		for(int i=0; i<size; i++)
		{
			long adjacents = in.readLong();
			adjacent.add(adjacents);
		}
	}
}

public class Graph {
	public static class GraphMapper extends Mapper<Object,Text,LongWritable,Vertex> {
		@Override
		public void map ( Object key, Text value, Context context )
				throws IOException, InterruptedException {
			String[] data = value.toString().split(",");

			long VID = Long.parseLong(data[0]);
			Vertex vertex = new Vertex();
			for(int i=1; i<data.length; i++)
			{
				vertex.adjacent.add(Long.parseLong(data[i]));
			}
			vertex.vid = VID;
			vertex.group = VID;
			context.write(new LongWritable(vertex.vid), vertex);
		}
	}

	public static class GraphReducer extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {

		@Override
		public void reduce ( LongWritable Key, Iterable<Vertex> values,Context context )
				throws IOException, InterruptedException {
			for(Vertex v : values)
			{
				context.write((LongWritable)Key,new Vertex(v.tag,v.group,v.vid,v.adjacent));
			}
		}
	}

	public static class GraphMapper1 extends Mapper<LongWritable,Vertex,LongWritable,Vertex> {

		@Override
		public void map (LongWritable Key,Vertex values,Context context )
				throws IOException, InterruptedException {
			LongWritable ids = new LongWritable(values.vid);
			context.write(ids,values);
			for(Long vadj : values.adjacent)
			{
				context.write(new LongWritable(vadj),new Vertex((short)1,values.group));  
			}
		}
	}

	public static class GraphReducer1 extends Reducer<LongWritable,Vertex,LongWritable,Vertex> {

		@SuppressWarnings({ "unchecked", "rawtypes" })
		@Override
		public void reduce ( LongWritable key, Iterable<Vertex> values, Context context)
				throws IOException, InterruptedException {
			Vector<Long> adj = new Vector<Long>();
			long m=Long.MAX_VALUE;
			for(Vertex vertex : values)
			{
				if(vertex.tag == 0)
				{
					adj = (Vector)vertex.adjacent.clone();
				}
				m = Math.min(m,vertex.group);
			}
			context.write(new LongWritable(m),new Vertex((short)0,m,key.get(),adj));
		}
	}

	public static class GraphMapper2 extends Mapper<LongWritable,Vertex,LongWritable,LongWritable> {

		@Override
		public void map ( LongWritable group, Vertex values,Context context)
				throws IOException, InterruptedException {
			context.write(group,new LongWritable(1));
		}
	}

	public static class GraphReducer2 extends Reducer<LongWritable,LongWritable,LongWritable,LongWritable> {

		@Override
		public void reduce (LongWritable key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
			long size = 0;
			for(LongWritable v:values) {
				size = size+(v.get());
			}
			context.write(key, new LongWritable(size));
		}
	}

	public static void main ( String[] args ) throws Exception {
		runJobOne(args);
		for ( short i = 0; i < 5; i++ ) {
			runJobTwo(args, i);
		}
		runJobThree(args);
	}

	private static void runJobThree(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Job job = Job.getInstance();
		job.setJobName("MyJob2");
		job.setJarByClass(Graph.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(LongWritable.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(LongWritable.class);

		job.setMapperClass(GraphMapper2.class);
		job.setReducerClass(GraphReducer2.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		SequenceFileInputFormat.setInputPaths(job,new Path(args[1]+"/f5"));

		FileOutputFormat.setOutputPath(job,new Path(args[2]));	
		job.waitForCompletion(true);
	}

	private static void runJobTwo(String[] args, short i)
			throws IOException, InterruptedException, ClassNotFoundException {
		Job job = Job.getInstance();
		job.setJobName("MyJob1-" + i);
		job.setJarByClass(Graph.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Vertex.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Vertex.class);

		job.setMapperClass(GraphMapper1.class);
		job.setReducerClass(GraphReducer1.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		SequenceFileInputFormat.setInputPaths(job,new Path(args[1]+"/f"+i));
		SequenceFileOutputFormat.setOutputPath(job,new Path(args[1]+"/f"+(i+1)));	
		job.waitForCompletion(true);
	}

	private static void runJobOne(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
		Job job = Job.getInstance();
		job.setJobName("MyJob");
		job.setJarByClass(Graph.class);

		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Vertex.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Vertex.class);

		job.setMapperClass(GraphMapper.class);
		job.setReducerClass(GraphReducer.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(job,new Path(args[0]));
		SequenceFileOutputFormat.setOutputPath(job,new Path(args[1]+"/f0"));	
		job.waitForCompletion(true);
	}
}


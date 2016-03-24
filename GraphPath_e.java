import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;

public class GraphPath_e
{
    //Inner class to represent a node
    public static class Node
    {
        // the integer node id
        private String id;
        // the ids of all nodes theis node has a path to private String neighbours;
		private String neighbours;
        //
        private int distance;
        // the current node state
        private String state;
        
        //parse the text file representation into a Node object
        Node( Text t)
        {
            String[] parts = t.toString().split("\t");
            this.id = parts[0];
			if (parts[1].equals(""))
				this.neighbours = null;
			else
				this.neighbours = parts[1];

            if (parts.length<3 || parts[2].equals(""))
                this.distance = -1;
            else 
                this.distance = Integer.parseInt(parts[2]);

            if (parts.length<4 || parts[3].equals(""))
                this.state = "P";
            else
                this.state = parts[3];
        }
        
        //Create a node from a key and value object pair
		Node(Text key, Text value)
		{
			this(new Text(key.toString()+"\t"+value.toString()));
		}
		public String getId()
		{
			return this.id;
		}
		public String getNeighbours()
		{
			return this.neighbours;
		}
		public int getDistance()
		{
			return this.distance;
		}
		public String getState()
		{
			return this.state;
		}
	}

	public static class GraphPathMapper extends Mapper<Object, Text, Text, Text>
	{
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException
		{
			Node n = new Node(value);
			if (n.getState().equals("C"))
			{
				//Output the node with its state changed to Done
				context.write(new Text(n.getId()), new Text(n.getId()+"\t"+n.getNeighbours()+"\t"+n.getDistance()+"\t"+"D"));
				for (String neighbour:n.getNeighbours().split(","))
				{
					//output each neighbour as a Currently precessing node
					//Increment the distance by 1; it is one link further away
					context.write(new Text(neighbour), new Text(neighbour+"\t"+"\t"+(n.getDistance()+1)+"\tC"));
				}
			}
			else
			{
				//output a pending node unchanged
				context.write(new Text(n.getId()), new Text(n.getId()+"\t"+n.getNeighbours()+"\t"+n.getDistance()+"\t"+n.getState()));
			}
		}
	}

	public static class GraphPathReducer extends Reducer<Text, Text, Text, Text>
	{
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
		{
			//set some default values for the final output
			String neighbours = null;
			int distance =-1;
			String state = "P";
			for (Text t:values)
			{
				Node n = new Node(t);
				if ( n.getState().equals("D"))
				{
					//a done node should be the final output; ignore the remaining values
					neighbours = n.getNeighbours();
					distance = n.getDistance();
					state = n.getState();
					break;
				}
				//select the list of neighbours when found
				if ( n.getNeighbours() != null )
					neighbours = n.getNeighbours();
				//select the largest distance
				if ( n.getDistance() > distance )
				{
					distance = n.getDistance();
				}
				//select the highest remaining state
				if ( n.getState().equals("D") || ( n.getState().equals("C") && state.equals("P")))
					state = n.getState();
			}
			//output a new node representation from the collected parts context parts
			context.write(key, new Text(neighbours+"\t"+distance+"\t"+state));
		}
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "graph path");
		job.setJarByClass(GraphPath_e.class);
		job.setMapperClass(GraphPathMapper.class);
		job.setReducerClass(GraphPathReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)?0:1);
	}
}






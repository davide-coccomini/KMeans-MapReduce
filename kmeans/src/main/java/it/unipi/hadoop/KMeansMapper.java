package it.unipi.hadoop;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.GenericOptionsParser;



public class KMeansMapper extends Mapper<Object, Text, Centroid, Point> {
    private List<Centroid> centroids = new ArrayList<Centroid>();
    
    /* SETUP: Read centroids from file and prepare the variables for the mapper */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        System.out.println("SETUP MAPPER");
        Configuration conf = context.getConfiguration();
        Path centroidsPath = new Path(conf.get("centroidsPath"));
        FileSystem fs = FileSystem.get(conf);
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroidsPath, conf);
        
        IntWritable key = new IntWritable();
        Centroid value = new Centroid();
        
        while(reader.next(key,value)){
            Centroid centroid = new Centroid(value.getCoordinates());
            centroid.setPointsCounter(new IntWritable(0));
            centroid.setIndex(key);
            centroids.add(centroid);
            
        }
        reader.close();
    }
    
    /* MAP: Find the nearest centroid to the considered point comparing it with all the centroids readed from the file */
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        System.out.println("MAPPER");
        String readedLine = value.toString();
        List<DoubleWritable> coordinates = new ArrayList<DoubleWritable>();
        StringTokenizer tokenizer = new StringTokenizer(readedLine, ",");
        
        while(tokenizer.hasMoreTokens()){
            coordinates.add(new DoubleWritable(Double.parseDouble(tokenizer.nextToken())));
        }
        
        Point point = new Point(coordinates);
        Centroid nearestCentroid = null; // Centroid with minimum distance from point 
        Double minDistance = Double.MAX_VALUE;
        Double tempDistance = null;
        
        for(Centroid centroid : centroids){
            tempDistance = centroid.calculateDistance(point);
            if(tempDistance < minDistance){
                minDistance = tempDistance;
                nearestCentroid = centroid;
            }
            context.write(nearestCentroid, point);
        }
    }
}
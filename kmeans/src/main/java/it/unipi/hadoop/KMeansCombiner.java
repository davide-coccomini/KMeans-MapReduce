package it.unipi.hadoop;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class KMeansCombiner extends Reducer<Centroid, Point, Centroid, Point> { 
    /* REDUCE: get a list of points and calculate the partial sum for each key. The key is the centroid associated to the list of points. */
    @Override
    public void reduce(Centroid centroid, Iterable<Point> points, Context context) throws InterruptedException, IOException {
        Configuration conf = context.getConfiguration();
        
        int counter = 0;
        Point partialSum = new Point(conf.getInt("dimension", 2)); // TODO: Evaluate complex class for partialSum
        
        for(Point point: points){
            for(int i=0; i<conf.getInt("dimension", 2); i++){
                double one = partialSum.getCoordinates().get(i).get();
                double two = point.getCoordinates().get(i).get();
                partialSum.getCoordinates().get(i).set(one + two); 
            }
            counter++;
        }
        centroid.setPointsCounter(new IntWritable(counter));
        context.write(centroid, partialSum);
    }
    
}

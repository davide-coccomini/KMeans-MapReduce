package it.unipi.hadoop;

import java.util.HashMap;
import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.fs.FileSystem;

public class KMeansReducer extends Reducer<Centroid, Point, IntWritable, Centroid> { 
    private HashMap<IntWritable, Centroid> oldCentroids = new HashMap<IntWritable, Centroid>();
    private HashMap<IntWritable, Centroid> newCentroids = new HashMap<IntWritable, Centroid>();
    private int convergedCentroids = 0;
    public enum COUNTER {CONVERGED}
    
    /* REDUCE: Gets a considered centroid as a key and a list of points. Calculate the new centroid starting from the partial sums. */
    /* Save the nw centroid and the old one in two hashmaps. These will be used in the cleanup function for the distance calculation. */
    @Override
    public void reduce(Centroid consideredCentroid, Iterable<Point> points, Context context) throws InterruptedException, IOException{
        Configuration conf = context.getConfiguration();
        Centroid newCentroid = new Centroid(conf.getInt("dimension", 2));
        boolean alreadySavedCentroid = false;
        
        if(newCentroids.containsKey(consideredCentroid.getIndex())){
            newCentroid = newCentroids.get(consideredCentroid.getIndex());
            alreadySavedCentroid = true;
        }
        
        int counterPoints = newCentroid.getPointsCounter().get();
        for(Point point : points) {
            for (int i = 0; i < point.getCoordinates().size(); i++) {
                newCentroid.getCoordinates().get(i).set(newCentroid.getCoordinates().get(i).get() + point.getCoordinates().get(i).get());
            }
            counterPoints += consideredCentroid.getPointsCounter().get();
        }
        newCentroid.setIndex(consideredCentroid.getIndex());
        newCentroid.setPointsCounter(new IntWritable(counterPoints));
        
        if(!alreadySavedCentroid){
            oldCentroids.put(consideredCentroid.getIndex(), new Centroid(consideredCentroid));
            newCentroids.put(newCentroid.getIndex(), newCentroid);
        }
    }
    /* CLEANUP: Starting from the hashmaps containg the old and new centroids, calculate the distances between them. */
    /* Check for the convergence of the centroids comparing the mean distance with a fixed treshold. */
    @Override
    protected void cleanup(Context context) throws InterruptedException, IOException {   
        Configuration conf = context.getConfiguration();
        Path centroidsPath = new Path(conf.get("centroidsPath"));
        
        FileSystem fs = FileSystem.get(conf);
        
        
        Writer.Option fileOption = Writer.file(centroidsPath);
        Writer.Option keyClassOption = Writer.keyClass(IntWritable.class);
        Writer.Option valueClassOption = Writer.valueClass(Centroid.class);
        
        Writer writer = SequenceFile.createWriter(conf, fileOption, keyClassOption, valueClassOption);
        
        int k = conf.getInt("k", 2);
        Iterator<Centroid> centroidsIterator = newCentroids.values().iterator();
        Centroid newCentroid;  // The new considered centroid
        Centroid oldCentroid; // The old centroid with the same index of newCentroid
        Double meanDistance = 0.0;
        Double threshold = Double.parseDouble(conf.get("threshold"));
        
        while(centroidsIterator.hasNext()){
            newCentroid = centroidsIterator.next();
            newCentroid.calculateCentroid();
            oldCentroid = oldCentroids.get(newCentroid.getIndex());
            if (threshold > newCentroid.calculateDistance(oldCentroid))
                convergedCentroids++;
            meanDistance += Math.pow(newCentroid.calculateDistance(oldCentroid), 2);
            writer.append(newCentroid.getIndex(), newCentroid);
            context.write(newCentroid.getIndex(), newCentroid);
        }
        
        writer.syncFs();
        writer.close();
        
        int percentSize = (newCentroids.size() * 90) / 100;
        meanDistance = Math.sqrt(meanDistance / k);
        if(convergedCentroids >= percentSize || meanDistance < threshold){
            context.getCounter(COUNTER.CONVERGED).increment(1);
        }
    }
}
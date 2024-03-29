package it.unipi.hadoop;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nonnull;


/* Class used as a base for centroid and to represent the points to be assigned to centroids. */
/* The class extends WritableComparable in order to allow a comparison between points. */
public class Point implements WritableComparable<Centroid> {
    private List<DoubleWritable> coordinates;
    protected IntWritable pointsCounter;
    
    Point() {
        coordinates = new ArrayList<DoubleWritable>();
        this.pointsCounter = new IntWritable(0);
    }
    
    Point(int dimension) {
        coordinates = new ArrayList<DoubleWritable>(dimension);
        for (int i = 0; i < dimension; i++) {
            coordinates.add(new DoubleWritable(0));
        }
        this.pointsCounter = new IntWritable(0);
    }
    
    Point(List<DoubleWritable> coordinates, IntWritable pointsCounter){
        this.coordinates = new ArrayList<DoubleWritable>();
        for(DoubleWritable coordinate: coordinates) {
            this.coordinates.add(new DoubleWritable(coordinate.get()));
        }
        this.pointsCounter = new IntWritable(pointsCounter.get());
    }
    Point(List<DoubleWritable> coordinates){
        this.coordinates = new ArrayList<DoubleWritable>();
        for(DoubleWritable coordinate: coordinates) {
            this.coordinates.add(new DoubleWritable(coordinate.get()));
        }
        this.pointsCounter = new IntWritable(1);
    }
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(coordinates.size());
        for (DoubleWritable p : coordinates) {
            dataOutput.writeDouble(p.get());
        }
        dataOutput.writeInt(pointsCounter.get());
    }
    
    public void readFields(DataInput dataInput) throws IOException {
        int dimension = dataInput.readInt();
        coordinates = new ArrayList<DoubleWritable>();
        for (int i = 0; i < dimension; i++) {
            coordinates.add(new DoubleWritable(dataInput.readDouble()));
        }
        pointsCounter = new IntWritable(dataInput.readInt());
    }
    
    public String toString() {
        String line = "";
        for(DoubleWritable coordinate : coordinates) {
            line += coordinate.get() + ",";
        }
        return line;
    }
    
    public List<DoubleWritable> getCoordinates() {
        return coordinates;
    }
    
    public Double calculateDistance(Point point){
        Double sum = 0.0;
        for (int i = 0; i < point.getCoordinates().size(); i++) {
            Double point_coordinates = point.getCoordinates().get(i).get();
            Double centroid_coordinates = this.getCoordinates().get(i).get();
            sum += Math.pow(centroid_coordinates - point_coordinates,  2);
        }
        return Math.sqrt(sum);
    }
    
    public int compareTo(@Nonnull Centroid centroid) {
        return 0;
    }
    IntWritable getPointsCounter() {
        return pointsCounter;
    }
    void setPointsCounter(IntWritable pointsCounter) {
        this.pointsCounter = new IntWritable(pointsCounter.get());
    }
}
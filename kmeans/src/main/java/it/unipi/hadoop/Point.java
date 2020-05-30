package it.unipi.hadoop;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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
    
    Point() {
        coordinates = new ArrayList<DoubleWritable>();
    }
    
    Point(int dimension) {
        coordinates = new ArrayList<DoubleWritable>(dimension);
        for (int i = 0; i < dimension; i++) {
            coordinates.add(new DoubleWritable(0));
        }
    }
    
    Point(List<DoubleWritable> coordinates){
        this.coordinates = new ArrayList<DoubleWritable>();
        for(DoubleWritable coordinate: coordinates) {
            this.coordinates.add(new DoubleWritable(coordinate.get()));
        }
    }
    
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(coordinates.size());
        for (DoubleWritable p : coordinates) {
            dataOutput.writeDouble(p.get());
        }
    }
    
    public void readFields(DataInput dataInput) throws IOException {
        int dimension = dataInput.readInt();
        coordinates = new ArrayList<DoubleWritable>();
        for (int i = 0; i < dimension; i++) {
            coordinates.add(new DoubleWritable(dataInput.readDouble()));
        }
    }
    
    public String toString() {
        String line = "Coordinates: ";
        for(DoubleWritable coordinate : coordinates) {
            line += coordinate.get() + ",";
        }
        return line;
    }
    
    public List<DoubleWritable> getCoordinates() {
        return coordinates;
    }
    
    public Double calculateDistance(Point point){
        System.out.println("Calculate Distance");
        Double sum = 0.0;
        for (int i = 0; i < point.getCoordinates().size(); i++) {
            Double point_coordinates = point.getCoordinates().get(i).get();
            Double centroid_coordinates = this.getCoordinates().get(i).get();
            sum += Math.pow(centroid_coordinates - point_coordinates,  2);
        }
        System.out.println("Distance: " + sum);
        System.out.println("Point: " + point.toString());
        return Math.sqrt(sum);
    }
    
    public int compareTo(@Nonnull Centroid centroid) {
        return 0;
    }
  
}
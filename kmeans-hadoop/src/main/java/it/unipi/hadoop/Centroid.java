package it.unipi.hadoop;


import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.DoubleWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import javax.annotation.Nonnull;

public class Centroid extends Point{
    private IntWritable index;
    
    Centroid() {
        super();
        this.index = new IntWritable(0);
        this.pointsCounter = new IntWritable(0);
    }
    
    Centroid(int dimension) {
        super(dimension);
        this.index = new IntWritable(0);
        this.pointsCounter = new IntWritable(0);
    }
    
    Centroid(List<DoubleWritable> coordinates, IntWritable index, IntWritable pointsCounter) {  
        super(coordinates, pointsCounter);
        this.index = new IntWritable(index.get());
    }
    Centroid(List<DoubleWritable> coordinates) {
        super(coordinates, new IntWritable(0));
        this.index = new IntWritable(0);
    }
    
    Centroid(Centroid centroid){
        super(centroid.getCoordinates(), centroid.getPointsCounter());
        setIndex(centroid.getIndex());
    }
    
    public void readFields(DataInput dataInput) throws IOException {
        super.readFields(dataInput);
        index = new IntWritable(dataInput.readInt());
    }

    public void write(DataOutput dataOutput) throws IOException {
        super.write(dataOutput);
        dataOutput.writeInt(index.get());
    }
    
    @Override
    public int compareTo(@Nonnull Centroid centroid) {
        if(this.getIndex().get() == centroid.getIndex().get()){
            return 0;
        }
        return 1;
    }
    
    boolean converged(Centroid centroid, Double threshold){
        return threshold > this.calculateDistance(centroid);   
    }
    
    public String toString(){
        return this.getIndex() + " " + super.toString();
    }
    
    /* Calculate the division between the sum of the coordinates contained in a centroid and their number. */
    void calculateCentroid(){
        for(int i=0; i<this.getCoordinates().size(); i++) {
            Double centroid = this.getCoordinates().get(i).get() / pointsCounter.get();
            this.getCoordinates().set(i, new DoubleWritable(centroid));
        }
    }
    
    IntWritable getIndex() {
        return index;
    }
    void setIndex(IntWritable index) {
        this.index = new IntWritable(index.get());
    }
   
}
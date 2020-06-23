package it.unipi.hadoop;

import java.io.IOException;
import java.io.*;
import java.util.StringTokenizer;
import java.util.Random;
import java.util.List; 
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.DoubleWritable;

public class KMeans 
{
    private static void generateRandomCentroids(Configuration conf, int k, int dimension, int instances, Path centroidsPath, String fileName) throws IOException{
        System.out.println("Generate Random Centroids");
        FileSystem fs = FileSystem.get(conf);
        
        
        Writer.Option fileOption = Writer.file(centroidsPath);
        Writer.Option keyClassOption = Writer.keyClass(IntWritable.class);
        Writer.Option valueClassOption = Writer.valueClass(Centroid.class);
        
        Writer writer = SequenceFile.createWriter(conf, fileOption, keyClassOption, valueClassOption);
 
        try{
            
            Random generator = new Random();
            for(int index = 0; index < k; index++){
                BufferedReader br = new BufferedReader(new FileReader(fileName));
                String line = null;
                
                int linesToSkip = (generator.nextInt(instances-1));
                
                for(int i = 0; i <= linesToSkip; i++){
                    line = br.readLine();
                }
                System.out.println("Line:" + line);
                String[] coordinates_strings = line.toString().split(",");
                List<DoubleWritable> coordinates = new ArrayList<DoubleWritable>();
                for(int j = 0; j < dimension; j++){
                    coordinates.add(new DoubleWritable(Double.parseDouble(coordinates_strings[j])));
                }
                Centroid centroid = new Centroid(coordinates, new IntWritable(index), new IntWritable(0));
                System.out.println("Centroid:" + centroid.toString());
                writer.append(new IntWritable(index), centroid);
                br.close();
            }
            
        }
        catch(IOException ex){
            System.out.println("Error reading file");
            ex.printStackTrace();
        }
          
        
      
        
        writer.syncFs();
        writer.close();
        
        fs = FileSystem.get(conf);
        
        SequenceFile.Reader reader = new SequenceFile.Reader(fs, centroidsPath, conf);
        IntWritable key = new IntWritable();
        Centroid value = new Centroid();
        
        System.out.println("Centroids from seq file:");
        while(reader.next(key,value)){
            System.out.println(value.toString());
        }
        reader.close();
        
        
    } 
    
    public static void main( String[] args )
    {
        System.out.println("Main method");
	 if(args.length != 6){
                System.out.println("Usage: KMeans.jar <k> <d> <n> <input> <output> <threshold>");
        }

        try {
            final Configuration conf = new Configuration();
            FileSystem hdfs = FileSystem.get(conf);

            int k = Integer.parseInt(args[0]);
            conf.setInt("k", k);
            int dimension = Integer.parseInt(args[1]);
            conf.setInt("dimension", dimension);

            Path inputPath = new Path(args[3]);
            Path outputPath = new Path(args[4]);
            Path centroidsPath = new Path("centroids/centroids.seq");

            generateRandomCentroids(conf, k, dimension, Integer.parseInt(args[2]), centroidsPath, args[3]);

            boolean converged = false;

            conf.set("centroidsPath", centroidsPath.toString());
            conf.set("threshold", args[5]);
            
            Job job = null;
            int iteration = 0;
            while(!converged){
                System.out.println("Iteration: " + iteration);
                
                
                hdfs = FileSystem.get(conf);

                SequenceFile.Reader reader = new SequenceFile.Reader(hdfs, centroidsPath, conf);
                IntWritable key = new IntWritable();
                Centroid value = new Centroid();

                int counter = 0;
                System.out.println("Centroidi");
                while(reader.next(key,value)){
                    System.out.println(counter++);
                }
                reader.close();
                
                
                job = new Job(conf, "kmeans");

                FileInputFormat.addInputPath(job, inputPath);
                FileOutputFormat.setOutputPath(job, outputPath);

                job.setJarByClass(KMeans.class);


                job.setMapperClass(KMeansMapper.class);
                job.setCombinerClass(KMeansCombiner.class);
                job.setReducerClass(KMeansReducer.class);
                
                job.setOutputKeyClass(IntWritable.class);
                job.setOutputValueClass(Centroid.class);
                job.setMapOutputKeyClass(Centroid.class);
                job.setMapOutputValueClass(Point.class);
                
                job.setNumReduceTasks(1);

                boolean completed = job.waitForCompletion(true);


                if(!completed){
                    System.out.println("Errore durante l'esecuzione del Job");
                    System.exit(0);
                }
                converged = (job.getCounters().findCounter(KMeansReducer.COUNTER.CONVERGED).getValue() == 1);
                
                
                iteration++;
                if (hdfs.exists(outputPath) && !converged){ //clean the output folder for a new execution
                  hdfs.delete(outputPath, true);
                }
                
            }
            
        }catch(Exception e){
            e.printStackTrace();
        }
    }
   

}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class UpdateJobRunner
{
    /**
     * Create a map-reduce job to update the current centroids.
     * @param jobId Some arbitrary number so that Hadoop can create a directory "<outputDirectory>/<jobname>_<jobId>"
     *        for storage of intermediate files.  In other words, just pass in a unique value for this
     *        parameter.
     * @param The input directory specified by the user upon executing KMeans, in which the points
     *        to find the KMeans point files are located.
     * @param The output directory for which to write job results, specified by user.
     * @precondition The global centroids variable has been set.
     */
    public static Job createUpdateJob(int jobId, String inputDirectory, String outputDirectory)
        throws IOException {
    	
    	Job init_job = new Job(new Configuration(), Integer.toString(jobId));
        init_job.setJarByClass(KMeans.class);
        
        //Setting up the mapper
        init_job.setMapperClass(PointToClusterMapper.class);
        init_job.setMapOutputKeyClass(IntWritable.class);
        init_job.setMapOutputValueClass(Point.class);
        
        //Setting up the reducer
        init_job.setReducerClass(ClusterToPointReducer.class);
        
        //init_job.setOutputKeyClass(IntWritable.class);
        //init_job.setOutputValueClass(Point.class);
        
        FileInputFormat.addInputPath(init_job, new Path(inputDirectory));
        FileOutputFormat.setOutputPath(init_job, new Path(outputDirectory+"/"+Integer.toString(jobId)));
        init_job.setInputFormatClass(KeyValueTextInputFormat.class);
        return init_job;
    }

    /**
     * Run the jobs until the centroids stop changing.
     * Let C_old and C_new be the set of old and new centroids respectively.
     * We consider C_new to be unchanged from C_old if for every centroid, c, in 
     * C_new, the L2-distance to the centroid c' in c_old is less than [epsilon].
     *
     * Note that you may retrieve publically accessible variables from other classes
     * by prepending the name of the class to the variable (e.g. KMeans.one).
     *
     * @param maxIterations   The maximum number of updates we should execute before
     *                        we stop the program.  You may assume maxIterations is positive.
     * @param inputDirectory  The path to the directory from which to read the files of Points
     * @param outputDirectory The path to the directory to which to put Hadoop output files
     * @return The number of iterations that were executed.
     */
    public static int runUpdateJobs(int maxIterations, String inputDirectory,
        String outputDirectory) throws IOException, InterruptedException, ClassNotFoundException {
        
    	ArrayList<Point> current_centroids = new ArrayList<Point>();
    	current_centroids.addAll(KMeans.centroids); 
    			
    	for(int i=0; i<maxIterations; i++){
    		
			System.out.println("Creating job with id "+i);
    		Job init_job = createUpdateJob(i, inputDirectory, outputDirectory);
    		init_job.waitForCompletion(true);
    		
    		ArrayList<Point> new_centroids = KMeans.centroids;
    		System.out.println(new_centroids.get(0).toString());
    		
    		//If the centroids have not changed in this iteration, we exit
    		if(!isCentroidChanged(current_centroids, new_centroids)){
    			System.out.println("Centroids have not changed in this iteration. Thus exiting");
    			return i+1;
    		}
    		
    		//Clearing the current centroids and making the new centroids the current ones
    		//for the next iteration
    		current_centroids.clear();
    		current_centroids.addAll(new_centroids);		
    	}
    	return maxIterations;
    }
    
    /**
     * Checks to see if the old and new centroid are the same or have changed
     */
    public static boolean isCentroidChanged(ArrayList<Point> old_centroid, ArrayList<Point> new_centroid) {
    	
    	//If the dimensions are not the same, the centroids have definitely changed
    	if (old_centroid.size() != new_centroid.size()) 
    		return true;
    	
    	//Return true even if one of the centroids is different
    	for (int i = 0; i < old_centroid.size(); i++) {
    		if (old_centroid.get(i).compareTo(new_centroid.get(i)) != 0) {
    			return true;
    		}
    	}
    	return false;	
    }
}

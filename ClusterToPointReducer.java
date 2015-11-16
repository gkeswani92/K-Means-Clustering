import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;
import java.io.IOException;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;

/** 
 * You can modify this class as you see fit, as long as you correctly update the
 * global centroids.
 */
public class ClusterToPointReducer extends Reducer<IntWritable, Point, Text, Text> {
	
	public void reduce(IntWritable key, Iterable<Point> values, Context context){
		
		Log log = LogFactory.getLog(ClusterToPointReducer.class);
		log.info("Inside reducer");
		
		ArrayList<Point> centroids = KMeans.centroids;
		
		//Finds the mean of all the points to create a new centroid for the given cluster
		int counter = 0;
		Point new_centroid = null;
		
		//New centroid for all the point in the cluster is the mean of all the points
		for(Point p: values){
			
			if(counter == 0)
				new_centroid = new Point(p.getDimension());

			new_centroid = Point.addPoints(new_centroid, p);
			counter++;
		}
		
		//If there are no points in this centroids cluster
		if(counter == 0 || new_centroid == null){
			return;
		}
		
		new_centroid = Point.multiplyScalar(new_centroid, 1.0f/(float)counter);
	
		//Replaces the current centroid with the new one
		centroids.set(key.get(), new_centroid);
	}			
}

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.util.ArrayList;
import java.io.IOException;

/** 
 * You can modify this class as you see fit, as long as you correctly update the
 * global centroids.
 */
public class ClusterToPointReducer extends Reducer<Integer, ArrayList<Point>, Text, Text> {
	
	public void reducer(Integer key, ArrayList<Point> values){
		
		ArrayList<Point> centroids = KMeans.centroids;
				
		//If this is the only point in this cluster, it should be the centroid
		if(values.size() == 1){
			centroids.set(key, values.get(0));
			return;
		}
		
		//Finds the mean of all the points to create a new centroid for the given cluster
		int num_points_in_cluster = values.size();
		Point new_centroid = new Point(values.get(0).getDimension());
		
		//New centroid for all the point in the cluster is the mean of all the points
		for(int i=0; i<num_points_in_cluster; i++){
			new_centroid = Point.addPoints(new_centroid, values.get(i));
		}
		new_centroid = Point.multiplyScalar(new_centroid, 1.0f/num_points_in_cluster);

		//Replaces the current centroid with the new one
		centroids.set(key, new_centroid);
	}
			
}

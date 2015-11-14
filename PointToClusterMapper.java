import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.ArrayList;
import java.io.IOException;

/**
 * You can modify this class as you see fit.  You may assume that the global
 * centroids have been correctly initialized.
 */
public class PointToClusterMapper extends Mapper<Text, Text, Integer, Point>
{
	/**
	 * @param key
	 * 		String representation of a point
	 * @param value
	 * 		Null
	 */
	public void map(Text key, Text value, Context context) {
		
		ArrayList<Point> centroids = KMeans.centroids;
		Point current_point = new Point(key.toString());
		
		Float min_distance = Float.MAX_VALUE;
		Point centroid_for_point  = null;
		
		//Finding the centroid which is closest to the current point
		for(Point centroid: centroids){
			Float current_distance = Point.distance(current_point, centroid);
			if(current_distance < min_distance){
				min_distance = current_distance;
				centroid_for_point = centroid;
			}
		}
		
		//Emits the index of the centroid this point is closest to along with 
		//the point itself
		Integer centroid_index = centroids.indexOf(centroid_for_point);
		
		try{
			context.write(centroid_index, current_point);
		} catch (InterruptedException e) {
			System.out.println("InterruptedException while emitting from mapper");
		} catch (IOException e) {
			System.out.println("IOException while emitting from mapper");
		}
		
	}
}

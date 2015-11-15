import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import java.util.ArrayList;
import java.io.IOException;

/**
 * You can modify this class as you see fit.  You may assume that the global
 * centroids have been correctly initialized.
 */
public class PointToClusterMapper extends Mapper<Text, Text, IntWritable, Point>
{
	/**
	 * @param key
	 * 		String representation of a point
	 * @param value
	 * 		Null
	 */
	public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
		
		Log log = LogFactory.getLog(PointToClusterMapper.class);
		log.info("Inside mapper");
		
		ArrayList<Point> centroids = KMeans.centroids;
		Point current_point = new Point(key.toString());
		
		Float min_distance = Float.MAX_VALUE;
		Point closest_centroid  = null;
		
		//Finding the centroid which is closest to the current point
		for(Point centroid: centroids){
			Float current_distance = Point.distance(current_point, centroid);
			if(current_distance < min_distance){
				min_distance = current_distance;
				closest_centroid = centroid;
			}
		}
		
		//Emits the index of the centroid this point is closest to along with 
		//the point itself
		Integer centroid_index = centroids.indexOf(closest_centroid);
		context.write(new IntWritable(centroid_index), current_point);
	}
}

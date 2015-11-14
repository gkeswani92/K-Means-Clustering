import java.io.*; // DataInput and DataOuput
import java.util.ArrayList;
import java.util.Collections;
import org.apache.hadoop.io.*; // Writable

/**
 * A Point is some ordered list of floats.
 * 
 * A Point implements WritableComparable so that Hadoop can serialize
 * and send Point objects across machines.
 *
 * NOTE: This implementation is NOT complete.  As mentioned above, you need
 * to implement WritableComparable at minimum.  Modify this class as you see fit.
 */
public class Point implements WritableComparable<Point> {
	
	private ArrayList<Float> coordinates;
	
	/**
	 * Default constructor
	 */
	public Point(){
		//Constructor present for the intermediate hadoop processes between 
		//the mapper and the reducer
		coordinates = new ArrayList<Float>();
	}
	
    /**
     * Construct a Point with the given dimensions [dimension]. The coordinates should all be 0.
     * For example:
     * Constructing a Point(2) should create a point (x_0 = 0, x_1 = 0)
     */
    public Point(int dimension)
    {
    	coordinates = new ArrayList<Float>();
        for (int i=0; i<dimension; i++){
            coordinates.add(0.0f);
        }
    }

    /**
     * Construct a point from a properly formatted string (i.e. line from a test file)
     * @param str A string with coordinates that are space-delimited.
     * For example: 
     * Given the formatted string str="1 3 4 5"
     * Produce a Point {x_0 = 1, x_1 = 3, x_2 = 4, x_3 = 5}
     */
    public Point(String str)
    {
    	coordinates = new ArrayList<Float>();
    	String input_coordinates[] = str.split(" ");
    	
        for (int i=0; i < input_coordinates.length ; i++){
            coordinates.add(Float.parseFloat(input_coordinates[i].trim()));
        }
    }

    /**
     * Copy constructor
     */
    public Point(Point other)
    {
    	coordinates = new ArrayList<Float>();
    	int num_coordinates = other.getDimension();
        Point p = new Point(num_coordinates);
        
        for(int i = 0; i<num_coordinates; i++){
        	coordinates.add(other.coordinates.get(i));
        }
    }

    /**
     * @return The dimension of the point.  For example, the point [x=0, y=1] has
     * a dimension of 2.
     */
    public int getDimension()
    {
    	return coordinates.size();
    }

    /**
     * Converts a point to a string.  Note that this must be formatted EXACTLY
     * for the autograder to be able to read your answer.
     * Example:
     * Given a point with coordinates {x=1, y=1, z=3}
     * Return the string "1 1 3"
     */
    public String toString()
    {
    	StringBuilder str = new StringBuilder();
    	
    	for(Float p: coordinates){
    		str.append(p);
    		str.append(" ");
    	}
    	
    	return str.toString().trim();
    }

    /**
     * One of the WritableComparable methods you need to implement.
     * See the Hadoop documentation for more details.
     * You should order the points "lexicographically" in the order of the coordinates.
     * Comparing two points of different dimensions results in undefined behavior.
     * 
     * Return 0 if the two points are the same
     */
    @Override
    public int compareTo(Point O)
    {   
    	int dim1 = O.getDimension();
        int dim2 = getDimension();

        //First checking if their dimensions are the same
        if (dim1 == dim2){
        	
        	//Checking if each coordinate is the same
            for(int i = 0; i<dim1; i++){
                if(coordinates.get(i) != O.coordinates.get(i)){
                    return 1;
                }
            }
            return 0;
        }
        return 1;
    }

    /**
     * @return The L2 distance between two points.
     */
    public static final float distance(Point x, Point y)
    {
        double sum = 0;
        
        for(int i = 0; i<x.getDimension(); i++){
            sum += Math.pow((x.coordinates.get(i) - y.coordinates.get(i)), 2);
        }
        
        double euclidean_distance = Math.sqrt(sum);
        return (float)euclidean_distance;
    }

    /**
     * @return A new point equal to [x]+[y]
     */
    public static final Point addPoints(Point x, Point y)
    {
    	if(x.getDimension()!= y.getDimension())
            return null;

        int num_coordinates = x.getDimension();
        Point p = new Point();
        
        //Adding the coordinates of x and y to create the coordinates of p
        for(int i = 0; i<num_coordinates; i++){
            p.coordinates.add(x.coordinates.get(i) + y.coordinates.get(i));
        }
        return p;
    }

    /**
     * @return A new point equal to [c][x]
     */
    public static final Point multiplyScalar(Point x, float c)
    {
    	int num_coordinates = x.getDimension();
        Point p = new Point();
        
        //Multiplying each coordinate of x with x
        for (int i = 0; i<num_coordinates; i++){
            p.coordinates.add(c * x.coordinates.get(i));
        }
        return p;
    }
    
    @Override
    public void write(DataOutput out) throws IOException {
    	
    	int num_coordinates = getDimension();
        out.writeInt(num_coordinates);
        for (int i=0; i < num_coordinates; i++){
            out.writeFloat(coordinates.get(i));
        }
    } 
    
    @Override
    public void readFields(DataInput in) throws IOException {
        
    	int num_coordinates = in.readInt();
        coordinates = new ArrayList<Float>();
        
        for (int i = 0; i < num_coordinates; i++){
            coordinates.add(in.readFloat());
        }
    }
}

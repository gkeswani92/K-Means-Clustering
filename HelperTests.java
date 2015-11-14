import org.junit.*;
import static org.junit.Assert.*;

public class HelperTests
{
    @Test
    public void distance_test_1()
    {
        // MyClass is tested
        Point p1 = new Point("0 0 0 0 0");
        Point p2 = new Point("3.0 3 3 0 3");

        // Tests
        assertEquals(6.0, Point.distance(p1, p2), 0.0001);
    }

    @Test
    public void distance_test_2()
    {
        Point p1 = new Point("2 2");
        Point p2 = new Point("2 2");

        // Tests
        assertEquals(0.0, Point.distance(p1, p2), 0.0001);
    }

    @Test
    public void distance_test_3()
    {
        Point p1 = new Point("2 2 4");
        Point p2 = new Point("2 2 0");

        // Tests
        assertEquals(4.0, Point.distance(p1, p2), 0.0001);
    }

    @Test
    public void point_add_test1()
    {
        Point p1 = new Point("0 0 0");
        Point p2 = new Point("1 2 2");
        Point p3 = new Point("2 1 1");
    
        assertEquals("1.0 2.0 2.0", Point.addPoints(p1, p2).toString());
        assertEquals("3.0 3.0 3.0", Point.addPoints(p2, p3).toString());
    }

    @Test
    public void point_mult_test1()
    {
        Point p1 = new Point("0 0 0");
        Point p2 = new Point("1 2 2");
        Point p3 = new Point("2 1 1");
    
        assertEquals("0.0 0.0 0.0", Point.multiplyScalar(p1, 3).toString());
        assertEquals("2.5 5.0 5.0", Point.multiplyScalar(p2, (float)2.5).toString());
    }
} 

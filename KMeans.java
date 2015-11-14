/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import java.io.BufferedReader;
import java.io.PrintWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;
import java.io.InputStreamReader;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class KMeans {
    // To keep things simple set up necessary global variables.
    // However, in a real, distributed Hadoop setup, one should pass around globals
    // through Configurations, Counters or files since globals will not be in sync across machines.
    // You can assume dimensions will be positive
    public static int dimension = 0;
    // You can assume that k will be positive.
    public static int k;
    // You should not change the order of this list.
    public static ArrayList<Point> centroids;
    public static String inputDirectory;
    public static String outputDirectory;
    public final static IntWritable one = new IntWritable(1);
    private final static Random rng = new Random();

    /**
     * Exit program if [b] is false, printing [msg] and a stack trace.
     */
    public static void assertTrue(boolean b, String msg)
    {
        if (!b)
        {
            System.err.println("Fatal Error: " + msg);
            Throwable t = new java.lang.Throwable();
            t.printStackTrace();
            System.exit(1);
        }
    }

    public static class FileMapper 
             extends Mapper<Text, Text, IntWritable, Point> {   
        /**
         * Map a file to points that all have the same key.
         * @param key     The string representing a point, with coordinates separated by spaces
         * @param value   Nothing
         */
        public void map(Text key, Text value, Context context)
            throws IOException, InterruptedException
        {
            Point p = new Point(key.toString());    
            assertTrue(p.getDimension() == dimension, "Invalid Dimension");
            // Map all the points to the same key, so reducer can find centroids
            context.write(KMeans.one, p);
        }
    }
    
    public static class FileReducer
        extends Reducer<IntWritable, Point, IntWritable, Point>
    {
        /**
         * Picks k points and makes them to the initial centroids.
         * Emits all the points unchanged with some arbitrary unique key.
         * @param set        Some arbitrary int.
         * @param values     A bunch of points    
         */
        public void reduce(IntWritable key, Iterable<Point> values, Context context)
            throws IOException, InterruptedException
        {
            ArrayList<Point> pts = new ArrayList<Point>();
            int counter = 0;
            for (Point p : values)
            {
                pts.add(new Point(p));
                context.write(new IntWritable(counter), p);
                ++counter;
            }
            assertTrue(k <= pts.size(), "k too high");

            // To choose k elements to be the starting centroids, we will shuffle the list and choose
            // the first k elements.
            Collections.shuffle(pts, rng);
            for (int i = 0; i < k; ++i)
            {
                centroids.set(i, pts.get(i));
            }
        }
    }

    /**
     * Create a map-reduce job whose sole purpose is to randomly choose k initial centroids
     * from file.
     */
    public static Job createInitializationJob(String inputDirectory, String outputDirectory)
        throws IOException
    {
        Job init_job = new Job(new Configuration(), "kmeans_init");
        init_job.setJarByClass(KMeans.class);
        init_job.setMapperClass(FileMapper.class);
        init_job.setMapOutputKeyClass(IntWritable.class);
        init_job.setMapOutputValueClass(Point.class);
        init_job.setReducerClass(FileReducer.class);
        init_job.setOutputKeyClass(IntWritable.class);
        init_job.setOutputValueClass(Point.class);
        FileInputFormat.addInputPath(init_job, new Path(inputDirectory));
        FileOutputFormat.setOutputPath(init_job, new Path(outputDirectory));
        init_job.setInputFormatClass(KeyValueTextInputFormat.class);
        return init_job;
    }

    /**
     * Read centroids from file and exit if there are any input errors.
     * @return A list of centroids that were read from [path].
     */
    public static ArrayList<Point> parseCentroidsFile(int expectedNumCentroids, int expectedDim, 
        String path) throws IOException
    {
        FileInputStream fis = new FileInputStream(path);
        BufferedReader br = new BufferedReader(new InputStreamReader(fis, Charset.forName("UTF-8")));
        String line;
        ArrayList<Point> pts = new ArrayList<Point>(expectedNumCentroids);
        while ((line = br.readLine()) != null && !line.isEmpty())
        {
            Point p = new Point(line);
            assertTrue(p.getDimension() == expectedDim, "Invalid Dimension");
            pts.add(p);
        }
        fis.close();
        assertTrue(pts.size() == expectedNumCentroids, "Invalid Number of Centroids");
        return pts;
    }

    public static void writeLineToFile(String filename, String contents)
        throws FileNotFoundException, UnsupportedEncodingException
    {
        PrintWriter writer = new PrintWriter(filename, "UTF-8");
        writer.println(contents);
        writer.close();
    }

    /**
     * There are two supported uses:
     * loading the centroids randomly and loading a fixed set of centroids from file.
     *
     * The functionality in here is traditionally called a "driver."
     * The driver creates map-reduce "jobs," which are parallelizable units of work
     * that run some combination of maps and reduces.
     * Jobs generally do not share any state as they can (and are ideally) run on
     * separate Java Virtual Machines.  However, in this homework, you will be running the code
     * on one machine so you may safely use global variables in this class to pass around
     * necessary information like the current centroids.
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        boolean centroidsFromFile = false;
        // Note that command line arguments are not sanitized
        if (otherArgs.length == 4 || otherArgs.length == 5)
        {
            k = Integer.parseInt(otherArgs[0]);
            dimension = Integer.parseInt(otherArgs[1]);
            inputDirectory = otherArgs[2];
            outputDirectory = otherArgs[3];
            if (otherArgs.length == 4)
            {
                centroids = new ArrayList<Point>(Collections.nCopies(k, new Point(dimension)));
            }
            else
            {
                centroidsFromFile = true;
                centroids = parseCentroidsFile(k, dimension, otherArgs[4]);
            }
        }
        else
        {
            System.err.println("Usage:");
            System.err.println("\tkmeans <k> <dim> <in directory> <out directory>");
            System.err.println("\tkmeans <k> <dim> <in directory> <out directory> <centroids>");
            System.err.println("Where:");
            System.err.println("\t<k> is the number of clusters");
            System.err.println("\t<dim> is the dimension of the input points");
            System.err.println("\t<in directory> is the input directory containing files of points to cluster");
            System.err.println("\t<out directory> is the output directory to which Hadoop files are written");
            System.err.println("\t<centroids> is the name of a single file containing the centroids.");
            System.err.println("\t\tIf this is provided, then initial centroids will not be arbitrarily chosen.");
            System.exit(2);
        }

        if (!centroidsFromFile)
        {
            Job initJob = createInitializationJob(inputDirectory, outputDirectory);
            initJob.waitForCompletion(true);
        }

        // At this point, the centroids have been initialized.  This calls the student implemented
        // Jobs.
        int iters = UpdateJobRunner.runUpdateJobs(10, inputDirectory, outputDirectory);

        System.out.println("============================");
        System.out.println("KMeans execution successful.");
        System.out.println("----------------------------");
        System.out.println("Number of Iterations: " + iters);
        System.out.println("Final centroids:" + centroids);
        System.out.println("============================");

        writeLineToFile("centroids", centroids.toString());

        System.exit(0);
    }
}

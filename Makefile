HADOOP=hadoop
MAIN=KMeans
JAVAC=javac
# Insert the path to the directory that CONTAINS the hadoop-2.7.1 folder with trailing /
# Thus, if your Hadoop directory structure looks like "/opt/hadoop-2.7.1/bin/...", then set HADOOP_PREFIX
# to "/opt/"
HADOOP_PREFIX=/usr/local/Cellar/hadoop/2.7.1/libexec
CLASS_PATH=$(HADOOP_PREFIX)/share/hadoop/common/*:$(HADOOP_PREFIX)/share/hadoop/yarn/lib/*:$(HADOOP_PREFIX)/share/hadoop/mapreduce/lib/*:$(HADOOP_PREFIX)/share/hadoop/mapreduce/*:./
JFLAGS = -classpath $(CLASS_PATH) #Xlint:deprecation
# Point this to your JUnit 4 path
JUNIT4_PATH=/usr/share/java/junit4.jar

JAVA_FILES = $(shell ls *.java)
CLASS_FILES = $(JAVA_FILES:.java=.class)

# Type "make" to compile.  This will also remove your output directory.
default: $(CLASS_FILES)
	jar cvf $(MAIN).jar *.class
	rm -rf output/

$(CLASS_FILES): %.class:%.java
	$(JAVAC) $(JFLAGS) $<

# Type "make clean" to remove the .class, .jar and output/ files
clean:
	rm -rf *.class
	rm -rf *.jar
	rm -rf output/

# Type "make testpoints" to run the unit tests for Point.java
# Note that this requires JUnit4 to be installed.
testpoints:
	javac -cp .:$(JUNIT4_PATH):$(CLASS_PATH) Point.java HelperTests.java
	java -cp .:$(JUNIT4_PATH):$(CLASS_PATH) org.junit.runner.JUnitCore HelperTests

#### NOTE: For the following tests, you need to compile your code first before running them
# Type "make ktest1" to run the first test, which is using 4-means clustering on 4-dimensional points.
# Make sure to compile first.
# The expected output is [0.0 1.0 1.0 1.0, 1.0 1.0 1.0 0.0, 1.0 1.0 0.0 1.0, 1.0 0.0 1.0 1.0]
ktest1:
	rm -rf output/
	rm -f centroids
	CLASSPATH=$(CLASS_PATH) $(HADOOP) jar $(MAIN).jar $(MAIN) 4 4 kmeans_test1/points/points1.txt output

# Type "make ktest2" to run the second test, which has one cluster in each of the 4 quadrants.
# Make sure to compile first.
# The expected output is [[1.5 1.5, -4.5 -4.5, 6.0 -7.0, -1.0 2.3500001]
ktest2:
	rm -rf output/
	rm -f centroids
	CLASSPATH=$(CLASS_PATH) $(HADOOP) jar $(MAIN).jar $(MAIN) 4 2 kmeans_test2/points/points1.txt output kmeans_test2/centroids/centroids.txt
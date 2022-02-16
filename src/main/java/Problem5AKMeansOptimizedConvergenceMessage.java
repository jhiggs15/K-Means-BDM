import com.google.gson.Gson;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Problem5AKMeansOptimizedConvergenceMessage {

        public static class KMeansMapper
                extends Mapper<Object, Text, Text, CoordinateAverage> {

            private Center[] centroids;

            @Override
            protected void setup(Mapper<Object, Text, Text, CoordinateAverage>.Context context) throws IOException, InterruptedException {
                super.setup(context);
                String centroidGson = context.getConfiguration().get("centroids");
                Gson gson = new Gson();
                centroids = gson.fromJson(centroidGson, Center[].class);
                for(Center center : centroids) {
                    // writes out each centroid with dummy values, so the centroids don't disappear
                    context.write(new Text(center.toString()), new CoordinateAverage());
                }
            }

            public void map(Object key, Text value, Context context
            ) throws IOException, InterruptedException {

                final String[] columns = value.toString().split(",");
                int x = Integer.parseInt(columns[0]);
                int y = Integer.parseInt(columns[1]);

                Center closestCenter = null;
                double smallestValue = Integer.MAX_VALUE;
                for (Center center : centroids) {
                    double proximity = center.proximityToCenter(x, y);
                    if(proximity < smallestValue) {
                        closestCenter = center;
                        smallestValue = proximity;
                    }
                }

                context.write(new Text(closestCenter.toString()), new CoordinateAverage(x, y));

            }


        }

        public static class KMeansCombiner
                extends Reducer<Text,CoordinateAverage,Text,CoordinateAverage> {

            @Override
            protected void reduce(Text key, Iterable<CoordinateAverage> values, Reducer<Text, CoordinateAverage, Text, CoordinateAverage>.Context context) throws IOException, InterruptedException {
                CoordinateAverage total = new CoordinateAverage();
                for(CoordinateAverage coordinateAverage : values) {
                    if(!coordinateAverage.getIsOnlyValue())
                        total.merge(coordinateAverage);
                }
                context.write(key, total);
            }


        }

        public static class KMeansReducer
                extends Reducer<Text,CoordinateAverage,Text,Text> {
            @Override
            protected void reduce(Text key, Iterable<CoordinateAverage> values, Reducer<Text, CoordinateAverage, Text, Text>.Context context) throws IOException, InterruptedException {
                CoordinateAverage total = new CoordinateAverage();
                Text newCenter;
                for(CoordinateAverage coordinateAverage : values) {
                    if(!coordinateAverage.getIsOnlyValue())
                        total.merge(coordinateAverage);
                }
                if(total.getIsOnlyValue()) newCenter = key;
                else newCenter = new Text(total.getAverageX() + "," + total.getAverageY());

                context.write(newCenter, key);

            }

        }


    // args [0]  : data
    // args [1]  : number initial centers (k)
    // args [2]  : output file : format of outfile is newCenter  oldCenter
    //              arg 3 cannot have any periods in path
    // args [3]  : number of iterations : leave this argument out or put a 1 for a single iteration
    //              otherwise put the number of iterations
        public static void main(String[] args) throws Exception {
            long timeNow = System.currentTimeMillis();

            String centroids = CommonFunctionality.getSerializedCenters(args[1]);
            int numberOfIterations = args.length > 3 ? Integer.parseInt(args[3]) : 1;

            for (int r = 1; r <= numberOfIterations; r++) {
                Job KMeanJob;
                if(r == 1) {
                    KMeanJob = CommonFunctionality.createKMeansJobWithCombiner(
                            args[0],
                            args[2] + r,
                            centroids,
                            KMeansMapper.class, CoordinateAverage.class, KMeansReducer.class, KMeansCombiner.class);

                    KMeanJob.waitForCompletion(true);
                }
                else {
                    String lastIterationsCenters = getSerializedCenters(args[2] + (r - 1 + "/part-r-00000"));
                    KMeanJob = CommonFunctionality.createKMeansJobWithCombiner(
                            args[0],
                            args[2] + r,
                            lastIterationsCenters,
                            KMeansMapper.class, CoordinateAverage.class, KMeansReducer.class, KMeansCombiner.class);

                    KMeanJob.waitForCompletion(true);

                }
                writeConvergence(args[2], r);

            }

            long timeFinish = System.currentTimeMillis();
            double seconds = (timeFinish - timeNow) / 1000.0;
            System.out.println(seconds + " seconds");

        }

        public static String getSerializedCenters(String filename) throws FileNotFoundException {
            Gson gson = new Gson();
            return gson.toJson(getCenters(filename));
        }

        public static List<Center> getCenters(String filename) throws FileNotFoundException {
            List<Center> listOfCenters = new ArrayList<>();

            String[] centers = seperateCentersFromFile(filename);


            for(String center : centers) {
                String[] xAndY = center.split(",");
                int x = Integer.parseInt(xAndY[0]), y = Integer.parseInt(xAndY[1]);
                listOfCenters.add(new Center(x, y));
            }

            return listOfCenters;

        }

        public static String[] seperateCentersFromFile(String filename) throws FileNotFoundException {
            BufferedReader reader = new BufferedReader(new FileReader(filename));
            String file = reader.lines().reduce((total, line) -> total + "\n" + line).get();
            String[] centers = file.replaceAll("\t", ",").split("\n");
            centers = Arrays.copyOfRange(centers, 1, centers.length);
            return centers;
        }

        private static void writeConvergence(String outputfile, int r) throws IOException {
            // the last file we wrote to was outputfile+(r-1)
            // new file we wrote to was outputfile+r

            String[] centers = CommonFunctionality.seperateCentersFromFile(outputfile + r + "/part-r-00000");
            for(String center : centers) {
                String[] currentCenters = center.split(",");
                int newCenterX = Integer.parseInt(currentCenters[0]);
                int newCenterY = Integer.parseInt(currentCenters[1]);
                int oldCenterX = Integer.parseInt(currentCenters[2]);
                int oldCenterY = Integer.parseInt(currentCenters[3]);

                if(Math.abs(newCenterX - oldCenterX) >= 100 ||
                        Math.abs(newCenterY - oldCenterY) >= 100) {
                    writeToFirstLine(outputfile, r, "Has not converged");
                    return;
                }

            }

            writeToFirstLine(outputfile, r, "Has converged");

        }

        // adapted from https://stackoverflow.com/questions/7339342/copy-a-string-to-the-beginning-of-a-file-in-java
        private static void writeToFirstLine(String outputfile, int r, String lineToWrite) throws IOException {
            BufferedReader reader = new BufferedReader(new FileReader(outputfile + r + "/part-r-00000"));
            // adds convergence line and removes old center
            String file = reader.lines().reduce("",(total, line) ->{
                String[] currentLine = line.replaceAll("\t", ",").split(",");
                return total + currentLine[0] + "," + currentLine[1] + "\n";
            });
            file = lineToWrite + "\n" + file;
            Files.write( Paths.get(outputfile + r + "/part-r-00000"), file.getBytes());
        }
    }

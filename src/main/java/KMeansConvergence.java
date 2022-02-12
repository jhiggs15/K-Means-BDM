import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeansConvergence {

    public static class KMeansMapper
            extends Mapper<Object, Text, Text, Text> {

        private Center[] centroids;

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            super.setup(context);
            String centroidGson = context.getConfiguration().get("centroids");
            Gson gson = new Gson();
            centroids = gson.fromJson(centroidGson, Center[].class);
            for(Center center : centroids) {
                // writes out each centroid with dummy values, so the centroids don't disappear
                context.write(new Text(center.toString()), new Text("-1,-1"));
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

            context.write(new Text(closestCenter.toString()), value);

        }


    }

    public static class KMeansReducer
            extends Reducer<Text,Text,Text,Text> {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // find new center for cluster, new center is average all xs w/ average of all ys
            int sumX, sumY, count;
            sumX = sumY = count = 0;
            for(Text coordinate : values) {
                String[] xAndY = coordinate.toString().split(",");
                int curX = Integer.parseInt(xAndY[0]);
                int curY = Integer.parseInt(xAndY[1]);

                if(curX != -1) { // ensures that the current value is not a dummy value writen during setup
                    sumX += curX;
                    sumY += curY;
                    count++;
                }
            }
            Text newCenter;
            if(sumX == 0) newCenter = key;
            else newCenter = new Text((sumX / count) + "," + (sumY / count));

            context.write(newCenter, key);

        }

    }


    // args [0]  : data
    // args [1]  : number initial centers (k)
    // args [2]  : output file : format of outfile is newCenter  oldCenter
    //              arg 3 cannot have any periods in path
    // args [3]  : number of iterations
    public static void main(String[] args) throws Exception {
        String centroids = CommonFunctionality.getSerializedCenters(args[1]);
        int numberOfIterations = args.length > 3 ? Integer.parseInt(args[3]) : 1;

        for (int r = 1; r <= numberOfIterations; r++) {
            Job KMeanJob;
            if(r == 1) {
                KMeanJob = CommonFunctionality.createKMeansJob(
                        args[0],
                        args[2] + r,
                        centroids,
                        KMeansMapper.class, Text.class, KMeansReducer.class);
                KMeanJob.waitForCompletion(true);

            }
            else {
                String lastIterationsCenters = CommonFunctionality.getSerializedCenters(args[2] + (r - 1 + "/part-r-00000"));
                KMeanJob = CommonFunctionality.createKMeansJob(
                        args[0],
                        args[2] + r,
                        lastIterationsCenters,
                        KMeansMapper.class, Text.class, KMeansReducer.class);

                KMeanJob.waitForCompletion(true);
                if(allHaveChangedSignificantly(args[2], r)) continue;
                else break;

            }



        }
    }

    private static boolean allHaveChangedSignificantly(String outputfile, int r) throws FileNotFoundException {
        // the last file we wrote to was outputfile+(r-1)
        // new file we wrote to was outputfile+r

        String[] centers = CommonFunctionality.seperateCentersFromFile(outputfile + r + "/part-r-00000");
        for(String center : centers) {
            String[] currentCenters = center.split(",");
            int newCenterX = Integer.parseInt(currentCenters[0]);
            int newCenterY = Integer.parseInt(currentCenters[1]);
            int oldCenterX = Integer.parseInt(currentCenters[2]);
            int oldCenterY = Integer.parseInt(currentCenters[3]);

            if(Math.abs(newCenterX - oldCenterX) < 100 ||
                    Math.abs(newCenterY - oldCenterY) < 100)
                return false;

        }

        return true;

    }


}

    import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
    import org.apache.hadoop.io.BooleanWritable;
    import org.apache.hadoop.io.IntWritable;
    import org.apache.hadoop.io.Text;
    import org.apache.hadoop.io.WritableComparable;
    import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

    import java.io.*;
    import java.util.ArrayList;
import java.util.List;
    import java.util.Objects;

public class Problem4KMeansOptimized {

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
            }
            else {
                KMeanJob = CommonFunctionality.createKMeansJobWithCombiner(
                        args[0],
                        args[2] + r,
                        CommonFunctionality.getSerializedCenters(args[2] + (r - 1 + "/part-r-00000")),
                        KMeansMapper.class, CoordinateAverage.class, KMeansReducer.class, KMeansCombiner.class);
            }

            KMeanJob.waitForCompletion(true);
        }


        long timeFinish = System.currentTimeMillis();
        double seconds = (timeFinish - timeNow) / 1000.0;
        System.out.println(seconds + " seconds");
    }

}

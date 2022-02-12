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

public class KMeansOptimized {

        // adapted from http://hadooptutorial.info/creating-custom-hadoop-writable-data-type/
    public static class CoordinateAverage implements WritableComparable<CoordinateAverage> {
        private IntWritable sumOfX;
        private IntWritable sumOfY;
        private IntWritable numberOfPoints;
        private BooleanWritable isOnlyValue;


        //Default Constructor
        public CoordinateAverage()
        {
            this.sumOfX = new IntWritable();
            this.sumOfY = new IntWritable();
            this.numberOfPoints = new IntWritable();
            this.isOnlyValue = new BooleanWritable();
            this.isOnlyValue.set(true);
        }

        public CoordinateAverage(int x, int y)
        {
            this.sumOfX = new IntWritable();
            this.sumOfY = new IntWritable();
            this.numberOfPoints = new IntWritable();
            this.isOnlyValue = new BooleanWritable();
            update(x, y);
        }

        public void merge(CoordinateAverage otherCoordinate) {
            this.isOnlyValue.set(false);
            sumOfX.set(this.sumOfX.get() + otherCoordinate.sumOfX.get());
            sumOfY.set(this.sumOfY.get() + otherCoordinate.sumOfY.get());
            numberOfPoints.set(this.numberOfPoints.get() + otherCoordinate.numberOfPoints.get());

        }

        public void update(int x, int y) {
            this.isOnlyValue.set(false);
            sumOfX.set(sumOfX.get() + x);
            sumOfY.set(sumOfY.get() + y);
            numberOfPoints.set(numberOfPoints.get() + 1);
        }

        public int getAverageX() {
            return sumOfX.get() / numberOfPoints.get();
        }

        public int getAverageY() {
            return sumOfY.get() / numberOfPoints.get();
        }

        public boolean getIsOnlyValue() {
            return isOnlyValue.get();
        }

        @Override
        //overriding default readFields method.
        //It de-serializes the byte stream data
        public void readFields(DataInput in) throws IOException {
            sumOfX.readFields(in);
            sumOfY.readFields(in);
            numberOfPoints.readFields(in);
            isOnlyValue.readFields(in);
        }
        @Override
        //It serializes object data into byte stream data
        public void write(DataOutput out) throws IOException {
            sumOfX.write(out);
            sumOfY.write(out);
            numberOfPoints.write(out);
            isOnlyValue.write(out);
        }
        // do not think we will need
        @Override
        public int compareTo(CoordinateAverage o)
        {
            return -1;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof CoordinateAverage)
            {
                CoordinateAverage other = (CoordinateAverage) o;
                return sumOfX.get() == other.sumOfX.get()
                        && sumOfY.get() == other.sumOfY.get() && numberOfPoints.get() == other.numberOfPoints.get();
            }
            return false;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(sumOfX, sumOfY, numberOfPoints);
        }
    }

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
    // args [3]  : number of iterations
    public static void main(String[] args) throws Exception {
        String centroids = getSerializedCenters(args[1]);
        int numberOfIterations = args.length > 3 ? Integer.parseInt(args[3]) : 1;

        for (int r = 1; r <= numberOfIterations; r++) {
            Job KMeanJob;
            if(r == 1) {
                KMeanJob = createKMeansJob(
                        args[0],
                        args[2] + r,
                        centroids);
            }
            else {
                KMeanJob = createKMeansJob(
                        args[0],
                        args[2] + r,
                        getSerializedCenters(args[2] + (r - 1 + "/part-r-00000")) );
            }

            KMeanJob.waitForCompletion(true);
        }
    }

    public static Job createKMeansJob(String inputFile, String outputFile, String searlizedCenters) throws IOException {
        Configuration conf = new Configuration();
        conf.setStrings("centroids", searlizedCenters);
        Job job = Job.getInstance(conf, "K-Means");
        job.setJarByClass(KMeansOptimized.class);
        job.setMapperClass(KMeansMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CoordinateAverage.class);

        job.setCombinerClass(KMeansCombiner.class);

        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));

        return job;
    }

    public static String getSerializedCenters(String filename) throws FileNotFoundException {
        Gson gson = new Gson();
        return gson.toJson(getCenters(filename));
    }

    public static List<Center> getCenters(String filename) throws FileNotFoundException {
        List<Center> listOfCenters = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new FileReader(filename));
        String file = reader.lines().reduce((total, line) -> total + "\n" + line).get();
        String[] centers = file.replaceAll("\t", ",").split("\n");

        for(String center : centers) {
            String[] xAndY = center.split(",");
            int x = Integer.parseInt(xAndY[0]), y = Integer.parseInt(xAndY[1]);
            listOfCenters.add(new Center(x, y));
        }

        return listOfCenters;

    }
}

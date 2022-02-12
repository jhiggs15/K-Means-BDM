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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

    public class KMeansOptimizedConvergenceMessage {

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
            String centroids = CommonFunctionality.getSerializedCenters(args[1]);
            int numberOfIterations = args.length > 3 ? Integer.parseInt(args[3]) : 1;

            for (int r = 1; r <= numberOfIterations; r++) {
                Job KMeanJob;
                if(r == 1) {
                    KMeanJob = CommonFunctionality.createKMeansJobWithCombiner(
                            args[0],
                            args[2] + r,
                            centroids,
                            KMeansOptimized.KMeansMapper.class, CoordinateAverage.class, KMeansOptimized.KMeansReducer.class, KMeansOptimized.KMeansCombiner.class);

                    KMeanJob.waitForCompletion(true);
                }
                else {
                    String lastIterationsCenters = CommonFunctionality.getSerializedCenters(args[2] + (r - 1 + "/part-r-00000"));
                    KMeanJob = CommonFunctionality.createKMeansJobWithCombiner(
                            args[0],
                            args[2] + r,
                            lastIterationsCenters,
                            KMeansOptimized.KMeansMapper.class, CoordinateAverage.class, KMeansOptimized.KMeansReducer.class, KMeansOptimized.KMeansCombiner.class);

                    KMeanJob.waitForCompletion(true);

                }
                writeConvergence(args[2], r);

            }
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
            Files.write( Paths.get(outputfile + r + "/result.txt"), file.getBytes());
        }

        public static Job createKMeansJob(String inputFile, String outputFile, String searlizedCenters) throws IOException {
            Configuration conf = new Configuration();
            conf.setStrings("centroids", searlizedCenters);
            Job job = Job.getInstance(conf, "K-Means");
            job.setJarByClass(KMeansOptimizedConvergenceMessage.class);
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
    }
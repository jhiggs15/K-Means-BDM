import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.google.gson.Gson;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class KMeans {

    public static class KMeansMapper
            extends Mapper<Object, Text, Text, Text> {

        private Center[] centroids;

        @Override
        protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            super.setup(context);
            String centroidGson = context.getConfiguration().get("centroids");
            Gson gson = new Gson();
            centroids = gson.fromJson(centroidGson, Center[].class);

        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            final String[] columns = value.toString().split(",");
            int x = Integer.parseInt(columns[1]);
            int y = Integer.parseInt(columns[2]);

            Center closestCenter = null;
            double smallestValue = Integer.MAX_VALUE;
            for (Center center : centroids) {
                if(center.proximityToCenter(x, y) < smallestValue)
                    closestCenter = center;
            }

            context.write(new Text(closestCenter.toString()), value);

        }


    }

    public static class KMeansReducer
            extends Reducer<Text,Text,Text,Text> {
        // List of Centers

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            // find new center for cluster
            // average all xs
            // average all ys
            int sumX, sumY, count;
            sumX = sumY = count = 0;
            for(Text coordinate : values) {
                String[] xAndY = coordinate.toString().split(",");
                sumX += Integer.parseInt(xAndY[0]);
                sumY += Integer.parseInt(xAndY[1]);
                count++;
            }
            Text newCenter = new Text((sumX / count) + "," + (sumY / count));
            context.write(key,newCenter);

        }

    }


    // args [0]  : data
    // args [1]  : number initial centers (k)
    // args [2] : output file
    public static void main(String[] args) throws Exception {
//        long timeNow = System.currentTimeMillis();
        List<Center> listOfCenters = new ArrayList<>();

        BufferedReader reader = new BufferedReader(new FileReader(args[1]));
        String file = reader.lines().reduce((total, line) -> total + "\n" + line).get();
        String[] centers = file.split("\n");

        for(String center : centers) {
            String[] xAndY = center.split(",");
            int x = Integer.parseInt(xAndY[1]), y = Integer.parseInt(xAndY[2]);
            listOfCenters.add(new Center(x, y));
        }

        Gson gson = new Gson();
        String centroids = gson.toJson(listOfCenters);

        Configuration conf = new Configuration();
        conf.setStrings("centroids", centroids);
        Job job = Job.getInstance(conf, "K-Means");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(KMeansMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);

//        long timeFinish = System.currentTimeMillis();
//        double seconds = (timeFinish - timeNow) / 1000.0;
//        System.out.println(seconds + " seconds");
    }
}

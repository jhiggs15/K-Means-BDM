import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CommonFunctionality {
    public static Job createKMeansJob(String inputFile, String outputFile, String searlizedCenters, Class mapperClass,  Class mapperOutputClass, Class reducerClass) throws IOException {
        Configuration conf = new Configuration();
        conf.setStrings("centroids", searlizedCenters);
        Job job = Job.getInstance(conf, "K-Means");
        job.setJarByClass(KMeans.class);
        job.setMapperClass(mapperClass);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(mapperOutputClass);

        job.setReducerClass(reducerClass);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));

        return job;
    }

    public static Job createKMeansJobWithCombiner(String inputFile, String outputFile, String searlizedCenters, Class mapperClass, Class reducerClass, Class mapperOutputClass, Class combinerClass ) throws IOException {
        Job job = createKMeansJob(inputFile, outputFile, searlizedCenters, mapperClass, reducerClass, mapperOutputClass);
        job.setCombinerClass(combinerClass);
        return job;
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
        return centers;
    }


}

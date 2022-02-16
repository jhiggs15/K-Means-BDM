import org.apache.hadoop.io.*;
import org.fusesource.leveldbjni.All;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

public class AllCoordinates implements WritableComparable<AllCoordinates> {
    private CoordinateAverage coordinateAverage;
    private Text pointsToPrint;


    //Default Constructor
    public AllCoordinates()
    {
        this.coordinateAverage = new CoordinateAverage();
        this.pointsToPrint = new Text("");
    }

    public AllCoordinates(int x, int y)
    {
        this.coordinateAverage = new CoordinateAverage();
        this.pointsToPrint = new Text("(" + x + "," + y + ")");
        coordinateAverage.update(x, y);

        // TODO add array writable
    }

    public void merge(AllCoordinates otherCoordinate) {
        coordinateAverage.merge(otherCoordinate.coordinateAverage);
        if(pointsToPrint.toString().equals("")) pointsToPrint.set(otherCoordinate.pointsToPrint);
        else this.pointsToPrint.set(pointsToPrint.toString() + " " + otherCoordinate.pointsToPrint.toString());

    }

    public Text getCoordinates() {

        return pointsToPrint;
    }

    public long getAverageX() {
        return coordinateAverage.getAverageX();
    }

    public long getAverageY() {
        return coordinateAverage.getAverageY();
    }

    public boolean getIsOnlyValue() {
        return coordinateAverage.getIsOnlyValue();
    }

    @Override
    //overriding default readFields method.
    //It de-serializes the byte stream data
    public void readFields(DataInput in) throws IOException {
        coordinateAverage.readFields(in);
        pointsToPrint.readFields(in);
    }
    @Override
    //It serializes object data into byte stream data
    public void write(DataOutput out) throws IOException {
        coordinateAverage.write(out);
        pointsToPrint.write(out);

    }
    // do not think we will need
    @Override
    public int compareTo(AllCoordinates o)
    {
        return -1;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AllCoordinates)
        {
            AllCoordinates other = (AllCoordinates) o;
            return coordinateAverage.equals(other.coordinateAverage);
        }
        return false;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(coordinateAverage);
    }
}

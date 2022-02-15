import org.apache.hadoop.io.*;
import org.fusesource.leveldbjni.All;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class AllCoordinates implements WritableComparable<AllCoordinates> {
    private CoordinateAverage coordinateAverage;
    private ArrayWritable pointsToPrint;


    //Default Constructor
    public AllCoordinates()
    {
        this.coordinateAverage = new CoordinateAverage();
        this.pointsToPrint = new ArrayWritable(Text.class);
        pointsToPrint.set(new Writable[0]);
    }

    public AllCoordinates(int x, int y)
    {
        this.coordinateAverage = new CoordinateAverage();
        this.pointsToPrint = new ArrayWritable(Text.class);
        pointsToPrint.set(new Writable[0]);
        update(x, y);
        // TODO add array writable
    }

    public void merge(AllCoordinates otherCoordinate) {
        coordinateAverage.merge(otherCoordinate.coordinateAverage);
        Writable[] mergedList = mergeTwoArrays(pointsToPrint.get(), otherCoordinate.pointsToPrint.get());
        this.pointsToPrint.set(mergedList);

    }

    private Writable[] mergeTwoArrays(Writable[] array1, Writable[] array2) {
        Writable[] mergedList = new Writable[array1.length + array2.length];

        for (int currentListItems = 0; currentListItems < array1.length; currentListItems++) {
            mergedList[currentListItems] = array1[currentListItems];
        }
        for (int otherListItems = 0; otherListItems < array2.length; otherListItems++) {
            mergedList[otherListItems + array1.length] = array2[otherListItems];
        }

        return mergedList;
    }

    public void update(int x, int y) {
        coordinateAverage.update(x, y);
        Writable[] newValue = new Writable[1];
        newValue[0] = new Text("(" + x + "," + y + ")");

        Writable[] currentList = pointsToPrint.get();
        currentList = currentList != null ? currentList : new Writable[0];
        this.pointsToPrint.set(mergeTwoArrays(currentList, newValue));

    }

    public String getCoordinates() {
        Writable[] theseCoordiantes = pointsToPrint.get();
        StringBuilder stringBuilder = new StringBuilder();
        int index = 0;
        int sizeMinus1 = theseCoordiantes.length - 1;
        for (Writable coordinate : theseCoordiantes) {
            if(index == sizeMinus1) stringBuilder.append(coordinate.toString() + "\n");
            else stringBuilder.append(coordinate.toString() + " ");
            index++;
        }

        return stringBuilder.toString();
    }

    public int getAverageX() {
        return coordinateAverage.getAverageX();
    }

    public int getAverageY() {
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

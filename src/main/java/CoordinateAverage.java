import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

// adapted from http://hadooptutorial.info/creating-custom-hadoop-writable-data-type/
public class CoordinateAverage implements WritableComparable<CoordinateAverage> {
    private LongWritable sumOfX;
    private LongWritable sumOfY;
    private LongWritable numberOfPoints;
    private BooleanWritable isOnlyValue;


    //Default Constructor
    public CoordinateAverage()
    {
        this.sumOfX = new LongWritable();
        this.sumOfY = new LongWritable();
        this.numberOfPoints = new LongWritable();
        this.isOnlyValue = new BooleanWritable();
        this.isOnlyValue.set(true);
    }

    public CoordinateAverage(int x, int y)
    {
        this.sumOfX = new LongWritable();
        this.sumOfY = new LongWritable();
        this.numberOfPoints = new LongWritable();
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

    public long getAverageX() {
        return sumOfX.get() / numberOfPoints.get();
    }

    public long getAverageY() {
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
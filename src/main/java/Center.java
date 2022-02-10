public class Center {
    private int x;
    private int y;

    public Center(int x, int y) {
        this.x = x;
        this.y = y;
    }

    @Override
    public String toString() {
        return x + "," + y;
    }

    // we have a center at 1,2 we want to find how far the point (0,1) is from it
    public double proximityToCenter(int otherX, int otherY) {
        double dist = Math.pow( (Math.pow( x - otherX, 2) +  Math.pow(y - otherY, 2)) , .5);
        return dist;
    }

}


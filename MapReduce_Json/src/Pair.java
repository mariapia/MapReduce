/**
 * Created by mariapia on 18/11/15.
 */
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;

public class Pair implements WritableComparable<Pair> {

    private String first;
    private String second;

    public Pair(String first, String second) {
        this.first = first;
        this.second = second;
    }

    public Pair() {
        this("", "");
    }

    @Override
    public String toString() {
        //return this.hashCode() + "\t" + first + "\t" + second;
        return first + "," + second;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, first);
        WritableUtils.writeString(out, second);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = WritableUtils.readString(in);
        second = WritableUtils.readString(in);
    }

    @Override
    public int hashCode() {
        BigInteger bA = BigInteger.ZERO;
        BigInteger bB = BigInteger.ZERO;

        for(int i = 0; i < first.length(); i++) {
            bA = bA.add(BigInteger.valueOf(127L).pow(i+1).multiply(BigInteger.valueOf(first.codePointAt(i))));
        }

        for(int i = 0; i < second.length(); i++) {
            bB = bB.add(BigInteger.valueOf(127L).pow(i+1).multiply(BigInteger.valueOf(second.codePointAt(i))));
        }

        return bA.multiply(bB).intValue();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof Pair) {
            Pair other = (Pair) o;

            boolean result = ( first.compareTo(other.first) == 0 && second.compareTo(other.second) == 0 )
                    || ( first.compareTo(other.second) == 0 && second.compareTo(other.first) == 0 );

            return result;
        }
        return false;
    }

    @Override
    public int compareTo(Pair other) {
        if (( first.compareTo(other.first) == 0 && second.compareTo(other.second) == 0 )
                || ( first.compareTo(other.second) == 0 && second.compareTo(other.first) == 0 ) ) {
            return 0;
        } else {
            int cmp = first.compareTo( other.first );

            if (cmp != 0) {
                return cmp;
            }

            return second.compareTo( other.second );
        }
    }
}

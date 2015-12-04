/**
 * Created by mariapia on 18/11/15.
 */
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;

public class StringPair implements WritableComparable<StringPair>, DataOutput {

    private String first;
    private String second;

    public StringPair(String first, String second) {
        this.first = first;
        this.second = second;
    }

    public StringPair(StringPair x){
        this.first = x.first;
        this.second = x.second;
    }
    public StringPair() {

        this("", "");
    }

    public String returnFirst(){
        return this.first;
    }

    public String returnSecond(){
        return this.second;
    }

    public void first(String x){
        this.first = x;
    }

    public void second(String x){
        this.second = x;
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
        if (o instanceof StringPair) {
            StringPair other = (StringPair) o;

            boolean result = ( first.compareTo(other.first) == 0 && second.compareTo(other.second) == 0 )
                    || ( first.compareTo(other.second) == 0 && second.compareTo(other.first) == 0 );

            return result;
        }
        return false;
    }

    @Override
    public int compareTo(StringPair other) {
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

    @Override
    public void write(int i) throws IOException {

    }

    @Override
    public void write(byte[] bytes) throws IOException {

    }

    @Override
    public void write(byte[] bytes, int i, int i1) throws IOException {

    }

    @Override
    public void writeBoolean(boolean b) throws IOException {

    }

    @Override
    public void writeByte(int i) throws IOException {

    }

    @Override
    public void writeShort(int i) throws IOException {

    }

    @Override
    public void writeChar(int i) throws IOException {

    }

    @Override
    public void writeInt(int i) throws IOException {

    }

    @Override
    public void writeLong(long l) throws IOException {

    }

    @Override
    public void writeFloat(float v) throws IOException {

    }

    @Override
    public void writeDouble(double v) throws IOException {

    }

    @Override
    public void writeBytes(String s) throws IOException {

    }

    @Override
    public void writeChars(String s) throws IOException {

    }

    @Override
    public void writeUTF(String s) throws IOException {

    }
}

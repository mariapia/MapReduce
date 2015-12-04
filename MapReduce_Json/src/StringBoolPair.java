import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigInteger;

/**
 * Created by mariapia on 23/11/15.
 */
public class StringBoolPair implements WritableComparable<StringBoolPair>, DataOutput {

    private String first;
    private Boolean second;

    public StringBoolPair(String first, Boolean second) {
        this.first = first;
        this.second = second;
    }

    public StringBoolPair(StringBoolPair x){
        this.first = x.first;
        this.second = x.second;
    }

    public StringBoolPair(){

    }


    @Override
    public String toString() {
        //return this.hashCode() + "\t" + first + "\t" + second;
        return first + "," + second;
    }


    public String returnFirst(){
        return this.first;
    }

    public Boolean returnSecond(){
        return this.second;
    }

    public void first(String x){
        this.first = x;
    }

    public void second(Boolean x){
        this.second = x;
    }



    @Override
    public void write(DataOutput out) throws IOException {
        WritableUtils.writeString(out, first);
        WritableUtils.writeString(out, second.toString());

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        first = WritableUtils.readString(in);
        second = Boolean.parseBoolean(WritableUtils.readString(in));
    }

    @Override
    public int hashCode() {
        BigInteger bA = BigInteger.ZERO;
        BigInteger bB = BigInteger.ZERO;

        for(int i = 0; i < first.length(); i++) {
            bA = bA.add(BigInteger.valueOf(127L).pow(i+1).multiply(BigInteger.valueOf(first.codePointAt(i))));
        }

        for(int i = 0; i < second.toString().length(); i++) {
            bB = bB.add(BigInteger.valueOf(127L).pow(i+1).multiply(BigInteger.valueOf(second.toString().codePointAt(i))));
        }

        return bA.multiply(bB).intValue();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof StringBoolPair) {
            StringBoolPair other = (StringBoolPair) o;

            boolean result = ( first.compareTo(other.first) == 0 && second.compareTo(other.second) == 0 )
                    || ( first.compareTo(other.second.toString()) == 0 && second.toString().compareTo(other.first) == 0 );

            return result;
        }
        return false;
    }

    @Override
    public int compareTo(StringBoolPair other) {
        if (( first.compareTo(other.first) == 0 && second.compareTo(other.second) == 0 )
                || ( first.compareTo(other.second.toString()) == 0 && second.toString().compareTo(other.first) == 0 ) ) {
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

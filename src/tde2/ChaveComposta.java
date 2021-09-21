package tde2;

import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ChaveComposta implements WritableComparable<ChaveComposta> {
    private String key1;
    private String key2;

    public ChaveComposta() {
    }

    public ChaveComposta(String key1, String key2) {
        this.key1 = key1;
        this.key2 = key2;
    }

    public String getKey1() {
        return key1;
    }

    public void setKey1(String key1) {
        this.key1 = key1;
    }

    public String getKey2() {
        return key2;
    }

    public void setKey2(String key2) {
        this.key2 = key2;
    }

    @Override
    public String toString() {
        return key1 + " " + key2;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(key1);
        dataOutput.writeUTF(key2);
    }

    @Override
    public void readFields(DataInput DataInput) throws IOException {
        key1 = DataInput.readUTF();
        key2 = DataInput.readUTF();
    }

    @Override
    public int compareTo(ChaveComposta o) {
        return ComparisonChain.start().compare(key1, o.key1).compare(key2, o.key2).result();
    }
}

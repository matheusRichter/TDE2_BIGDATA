package tde2;

import com.google.common.collect.ComparisonChain;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ChaveTripla implements WritableComparable<ChaveTripla> {
    private String key1;
    private String key2;
    private String key3;

    public ChaveTripla() {
    }

    public ChaveTripla(String key1, String key2, String key3) {
        this.key1 = key1;
        this.key2 = key2;
        this.key3 = key3;
    }

    @Override
    public String toString() {
        return "Key1: " + key1 + "\nkey2: " + key2 + "\nKey3: " + key3 + "\nResultado: ";
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(key1);
        dataOutput.writeUTF(key2);
        dataOutput.writeUTF(key3);
    }

    @Override
    public void readFields(DataInput DataInput) throws IOException {
        key1 = DataInput.readUTF();
        key2 = DataInput.readUTF();
        key3 = DataInput.readUTF();
    }

    @Override
    public int compareTo(ChaveTripla o) {
        return ComparisonChain.start().compare(key1, o.key1).compare(key2, o.key2).compare(key3, o.key3).result();
    }
}

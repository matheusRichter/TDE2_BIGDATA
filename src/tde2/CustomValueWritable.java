package tde2;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CustomValueWritable implements WritableComparable<CustomValueWritable> {
    private long ocorrencia;
    private double soma;

    public CustomValueWritable() {
    }

    public CustomValueWritable(long ocorrencia, double soma) {
        this.ocorrencia = ocorrencia;
        this.soma = soma;
    }

    public long getOcorrencia() {
        return ocorrencia;
    }

    public void setOcorrencia(long ocorrencia) {
        this.ocorrencia = ocorrencia;
    }

    public double getSoma() {
        return soma;
    }

    public void setSoma(double soma) {
        this.soma = soma;
    }

    @Override
    public int compareTo(CustomValueWritable o) {
        if(this.hashCode() < o.hashCode()){
            return -1;
        }else if(this.hashCode() > o.hashCode()){
            return +1;
        }
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(ocorrencia);
        dataOutput.writeDouble(soma);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        ocorrencia = dataInput.readLong();
        soma = dataInput.readDouble();
    }
}

package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/*
* Java Bean
* 1) Todos os atributos são privados
* 2) possui getters/setter para alterar/acessar cada atributo
* 3) deve possuir um construtos vazio (construtor que não recebe nenhum parâmetro)
* */

public class FireAvgTempWritable implements WritableComparable {
    private float somaTemperatura;
    private int ocorrencia;

    public FireAvgTempWritable() {}

    public FireAvgTempWritable(float somaTemperatura, int ocorrencia) {
        this.somaTemperatura = somaTemperatura;
        this.ocorrencia = ocorrencia;
    }

    public float getSomaTemperatura() {
        return somaTemperatura;
    }

    public void setSomaTemperatura(float somaTemperatura) {
        this.somaTemperatura = somaTemperatura;
    }

    public int getOcorrencia() {
        return ocorrencia;
    }

    public void setOcorrencia(int ocorrencia) {
        this.ocorrencia = ocorrencia;
    }

    @Override
    public String toString() {
        return "FireAvgTempWritable{" +
                "somaTemperatura=" + somaTemperatura +
                ", ocorrencia=" + ocorrencia +
                '}';
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(somaTemperatura);
        dataOutput.writeInt(ocorrencia);
        // para texto usar dataOutput.writeUTF();
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        somaTemperatura = dataInput.readFloat();
        ocorrencia = dataInput.readInt();
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return super.equals(obj);
    }

    @Override
    public int compareTo(Object o) {
        if (this.hashCode() < o.hashCode()) return -1;
        else if (this.hashCode() > o.hashCode()) return 1;
        return 0; // meu objeto é igual ao que estou comparando
    }
}

package advanced.customwritable;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

// a classe deve ser Writable e Comparable
public class ForestFireWritable implements WritableComparable<ForestFireWritable> {
    // atributos privados
    private float temperatura;
    private float vento;

    // construtor vazio
    public ForestFireWritable() {}

    public ForestFireWritable(float temperatura, float vento) {
        this.temperatura = temperatura;
        this.vento = vento;
    }

    // getters e setter para acessar/modificar os atributos e métodos
    public float getTemperatura() { return temperatura; }

    public void setTemperatura(float temperatura) { this.temperatura = temperatura; }

    public float getVento() { return vento; }

    public void setVento(float vento) { this.vento = vento; }

    // sobrescrever alguns métodos importantes (compareTo, write, readFields, toString, equals, hashCode)
    @Override
    public String toString() {
        return "temperatura=" + temperatura +
                ", vento=" + vento;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ForestFireWritable that = (ForestFireWritable) o;
        return Float.compare(that.temperatura, temperatura) == 0 && Float.compare(that.vento, vento) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(temperatura, vento);
    }

    @Override
    public int compareTo(ForestFireWritable o) {
        if (this.hashCode() < o.hashCode()) return -1;
        else if (this.hashCode() > o.hashCode()) return 1;
        return 0;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeFloat(temperatura);
        dataOutput.writeFloat(vento);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        temperatura = dataInput.readFloat();
        vento = dataInput.readFloat();
    }
}

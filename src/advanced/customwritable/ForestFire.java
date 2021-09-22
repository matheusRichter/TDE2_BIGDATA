package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.io.Text;
import org.apache.log4j.BasicConfigurator;
import java.io.IOException;

public class ForestFire {

    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();

        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "forestfire");

        // registro das classes
        j.setJarByClass(ForestFire.class);
        j.setMapperClass(ForestFireMapper.class);
        j.setReducerClass(ForestFireReducer.class);

        // definição dos tipos de chave/valor de saída
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(ForestFireWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(ForestFireWritable.class);

        // definição dos arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class ForestFireMapper extends Mapper<Object, Text, Text, ForestFireWritable> {
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // convertendo cada linha (Text) para String e separando os campos em colunas
            String[] colunas = value.toString().split(",");

            // pegar os valores de mês, temperatura e vento
            String mes = colunas[2];
            float temperatura = Float.parseFloat(colunas[8]);
            float vento = Float.parseFloat(colunas[10]);

            // definindo a chave de saída do Map
            Text chaveSaida = new Text(mes);

            // enviando [mes, (temperatura, veto)] para o sort/shuffle -> reducer
            context.write(chaveSaida, new ForestFireWritable(temperatura, vento));
            context.write(new Text("anual"), new ForestFireWritable(temperatura, vento));
        }
    }

    public static class ForestFireReducer extends Reducer<Text, ForestFireWritable, Text, ForestFireWritable> {
        public void reduce(Text key, Iterable<ForestFireWritable> values, Context context) throws IOException, InterruptedException {
            float maiorTemperatura = -100;
            float maiorVento = -1;

            // percorrer todos os valores de uma chave (mês) e buscar pela maior temperatura e maior vento
            for (ForestFireWritable o : values) {
                // verificar se a maior temperatura
                if (o.getTemperatura() > maiorTemperatura)
                    maiorTemperatura = o.getTemperatura();

                // verificar se o maior vento
                if (o.getVento() > maiorVento)
                    maiorVento = o.getVento();
            }

            // escrever no HDFS, qual a chave (mês) e saída (maiorTemperatura, maiorVento)
            context.write(key, new ForestFireWritable(maiorTemperatura, maiorVento));
        }
    }
}

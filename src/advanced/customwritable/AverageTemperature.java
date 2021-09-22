package advanced.customwritable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class AverageTemperature {

    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "media");

        // registro das classes
        j.setJarByClass(AverageTemperature.class);
        j.setMapperClass(MapForAverage.class);
        j.setCombinerClass(CombinerForAverage.class);
        j.setReducerClass(ReduceForAverage.class);

        // definição dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(FireAvgTempWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(FloatWritable.class);

        // cadastro dos arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input);
        FileOutputFormat.setOutputPath(j, output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapForAverage extends Mapper<LongWritable, Text, Text, FireAvgTempWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // convertendo cada linha de entrada (Text) em string
            String linha = value.toString();

            // obtém tempratura que está armazenada na coluna 8
            String[] colunas = linha.split(",");

            float temperatura = Float.parseFloat(colunas[8]);
            //String mes = colunas[2];

            // ocorrência
            int n = 1;

            // passando a temperatura e ocorrência para o sort/shuffle -> reduce
            con.write(new Text("media"), new FireAvgTempWritable(temperatura, n));
            //con.write(new Text(mes), new FireAvgTempWritable(temperatura, n));
        }
    }

    public static class ReduceForAverage extends Reducer<Text, FireAvgTempWritable, Text, FloatWritable> {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con)
                throws IOException, InterruptedException {
            // chega no reduce, uma chave única (média) e a lista de valores compostos (temperatura, n)
            float somaTemperatura = 0;
            int somaNs = 0;

            for (FireAvgTempWritable o : values) {
                somaTemperatura += o.getSomaTemperatura();
                somaNs += o.getOcorrencia();
            }

            // salvar os resultados
            con.write(key, new FloatWritable(somaTemperatura/somaNs));
        }
    }

    public static class CombinerForAverage extends Reducer<Text, FireAvgTempWritable, Text, FireAvgTempWritable> {
        public void reduce(Text key, Iterable<FireAvgTempWritable> values, Context con) throws IOException, InterruptedException {
            // chega no reduce, uma chave única (média) e a lista de valores compostos (temperatura, n)
            float somaTemperatura = 0;
            int somaNs = 0;

            for (FireAvgTempWritable o : values) {
                somaTemperatura += o.getSomaTemperatura();
                somaNs += o.getOcorrencia();
            }

            // salvar os resultados
            con.write(key, new FireAvgTempWritable(somaTemperatura, somaNs));
        }
    }

}

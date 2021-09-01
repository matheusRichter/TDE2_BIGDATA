package basic;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class WordLengthCount {
    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();
        Configuration c = new Configuration();

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordlengthcount");

        // 1 - Registro das classes (definir quem é a classe principal, quem é o map, quem é o reduce)
        j.setJarByClass(WordLengthCount.class); // main
        j.setMapperClass(WordLengthCount.Map.class); // map
        j.setReducerClass(WordLengthCount.Reduce.class); // class

        // 2 - Definição dos tipos de saída (map e reduce)
        j.setMapOutputKeyClass(IntWritable.class); // tipo da chave de saída do map
        j.setMapOutputValueClass(IntWritable.class); // tipo do valor de saída do map

        j.setOutputKeyClass(IntWritable.class); // tipo da cahve de saída do reducer
        j.setOutputValueClass(IntWritable.class); // tipo do valor de saída do reducer

        // 3 - Cadastrar os arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input); // arquivo de entrada
        FileOutputFormat.setOutputPath(j, output); // arquivo de saída

        // 4 - Executar o job
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            // obtendo o conteúdo da linha
            String linha = value.toString().toLowerCase();
            linha = linha.replaceAll("[,.:;'\"\\)\\(!?^$*%\\-]|[0-9]", "");

            // quebrando a linha em palavras e armazenar em um vetro de strings
            String[] palavras = linha.split(" ");

            for (String p : palavras) {
                if (p.equals("  ")) continue;

                IntWritable chaveSaida = new IntWritable(p.length());
                IntWritable valorSaida = new IntWritable(1);

                con.write(chaveSaida, valorSaida);
            }
        }
    }

    public static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

        public void reduce(IntWritable key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
            // somando todos os valores de ocorrência para uma palavra específica (chave)
            int soma = 0;
            for (IntWritable v : values) {
                soma += v.get();
            }
            IntWritable valorSaida = new IntWritable(soma);

            // salvando (chave, valor)
            con.write(key, valorSaida);
        }
    }
}

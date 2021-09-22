package basic;

import java.io.IOException;

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
import org.apache.log4j.BasicConfigurator;


public class WordCount {

    public static void main(String[] args) throws Exception {

        BasicConfigurator.configure();
        Configuration c = new Configuration();

        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path(files[0]);

        // arquivo de saida
        Path output = new Path(files[1]);

        // criacao do job e seu nome
        Job j = new Job(c, "wordcount");

        // 1 - Registro das classes (definir quem é a classe principal, quem é o map, quem é o reduce)
        j.setJarByClass(WordCount.class); // main
        j.setMapperClass(MapForWordCount.class); // map
        j.setReducerClass(ReduceForWordCount.class); // class

        // 2 - Definição dos tipos de saída (map e reduce)
        j.setMapOutputKeyClass(Text.class); // tipo da chave de saída do map
        j.setMapOutputValueClass(IntWritable.class); // tipo do valor de saída do map

        j.setOutputKeyClass(Text.class); // tipo da cahve de saída do reducer
        j.setOutputValueClass(IntWritable.class); // tipo do valor de saída do reducer

        // 3 - Cadastrar os arquivos de entrada e saída
        FileInputFormat.addInputPath(j, input); // arquivo de entrada
        FileOutputFormat.setOutputPath(j, output); // arquivo de saída

        // 4 - Executar o job
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }

    /**
     * Parametro 1: tipo da chave de entrada
     * Parametro 2: tipo do valor da entrada
     * Parametro 3: tipo da chave de saida
     * Parametro 4: tipo do valor de saida
     *
     * ARQUIVO TEXTO (ENTRADA)
     * - Input: (offset, conteúdo da linha)
     * LongWritable é um long serializável. O objeto é transformado em bytes para ser
     * transferido na rede
     * Mesma coisa para Text (String serializável) e IntWritable
     */
    public static class MapForWordCount extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context con) throws IOException, InterruptedException {
            // obtendo o conteúdo da linha
            String linha = value.toString().toLowerCase();
            linha = linha.replaceAll("[,.:;'\"\\)\\(!?^$*%\\-]|[0-9]", "");

            // quebrando a linha em palavras e armazenar em um vetro de strings
            String[] palavras = linha.split(" ");

            // loop para criar (chave, valor) com (palavra, 1)
            for (String p : palavras) {
                if (p.equals("  ") || p.equals("")) continue;
                // (p, 1) -> chaveSaida, valorSaida
                Text chaveSaida = new Text(p);
                IntWritable valorSaida = new IntWritable(1);

                // Context é o responsável por enviar (chave, valor) para as próximas etapas (sort/shuffle -> reduce)
                // responsável por fazer a comunicação (transferência de chave, valor) entre os diferentes nós de
                // maneira transparente
                con.write(chaveSaida, valorSaida);
            }
        }
    }

    /**
     * Parâmetro 1: Tipo da chave de entrada (igual à chave de saída do Map - Text)
     * Parâmetro 2: Tipo do valor de entrada (igual ao valor de saída do Map - IntWritable)
     * Parâmetro 3: Tipo da chave de saída
     * Parâmetro 4: Tipo do valor de saída
     * */
    public static class ReduceForWordCount extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context con) throws IOException, InterruptedException {
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

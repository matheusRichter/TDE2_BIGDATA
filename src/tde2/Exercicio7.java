package tde2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Exercicio7 {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        // arquivo de entrada
        Path input = new Path("./in/operacoes.csv");

        // arquivo de saida
        Path output = new Path("./output/exe7.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "exe7");

        // registro das classes
        j.setJarByClass(Exercicio7.class);//main
        j.setMapperClass(MapForExe7.class);
        j.setReducerClass(ReduceForExe7.class);

        // definicao dos tipos de saida(map e reduce)
        j.setMapOutputKeyClass(ChaveComposta.class);
        j.setMapOutputValueClass(IntWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(IntWritable.class);

        // cadastrar erquivos de entada e saida
        FileInputFormat.addInputPath(j,input); // entrada
        FileOutputFormat.setOutputPath(j,output); // saida

        // executar o job
        System.exit(j.waitForCompletion(true) ? 0 : 1);

    }


    /**
     * Parametro (Tipo) 1: Tipo da chave de entrada
     * Parametro 2: Tipo do valor da entrada
     * Parametro 3: Tipo da chave de saida
     * Parametro 4: Tipo do valor de saida
     *
     * ARQUIVO TEXTO DE ENTRADA
     * - Input: (offset, conteudo da linha)
     *
     */
    public static class MapForExe7 extends Mapper<LongWritable, Text, ChaveComposta, IntWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            //convertendo a linha em string
            String linha = value.toString();

            //fazendo split para pegar valores
            String[] colunas = linha.split(";");

            String flow = colunas[4];
            String year = colunas[1];

            ChaveComposta chaveSaida = new ChaveComposta(flow,year);
            IntWritable valorSaida = new IntWritable(1);
            con.write(chaveSaida, valorSaida);
        }
    }

    /**
     * 1º tipo: tipo da chave de entrade (igual a chave de saida do map)
     * 2º tipo: tipo do valor de entrada (igual ao valor de saida do map)
     * 3º tipo: tipo da chave de saida
     * 4º tipo: tipo do valor de saida
     */

    public static class ReduceForExe7 extends Reducer<ChaveComposta, IntWritable, Text, IntWritable> {

        public void reduce(ChaveComposta key, Iterable<IntWritable> values, Context con)
                throws IOException, InterruptedException {

            //somando todos  os valores de ocorrencia para uma palavra especifica
            int soma = 0;
            for(IntWritable o : values){
                soma += o.get();
            }

            // criacao de chave e valor de saida
            IntWritable valorSaida = new IntWritable(soma);

            Text keyText = new Text(key.toString());

            con.write(keyText,valorSaida);
        }
    }
}

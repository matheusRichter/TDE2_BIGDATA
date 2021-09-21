package tde2;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Exercicio6 {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        // arquivo de entrada
        Path input = new Path("./in/operacoes.csv");

        // arquivo de saida
        Path output = new Path("./output/exe6.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "exe6");

        // registro das classes
        j.setJarByClass(Exercicio6.class);//main
        j.setMapperClass(MapForExe6.class);
        j.setReducerClass(ReduceForExe6.class);

        // definicao dos tipos de saida(map e reduce)
        j.setMapOutputKeyClass(ChaveComposta.class);
        j.setMapOutputValueClass(DoubleWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

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
    public static class MapForExe6 extends Mapper<LongWritable, Text, ChaveComposta, DoubleWritable> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            //convertendo a linha em string
            String linha = value.toString();

            //fazendo split para pegar valores
            String[] colunas = linha.split(";");

            if(NumberUtils.isParsable(colunas[5])) {
                double price = Double.parseDouble(colunas[5]);
                String unitType = colunas[7];
                String year = colunas[1];

                ChaveComposta chaveSaida = new ChaveComposta(unitType, year);
                con.write(chaveSaida, new DoubleWritable(price));
            }
        }
    }

    /**
     * 1º tipo: tipo da chave de entrade (igual a chave de saida do map)
     * 2º tipo: tipo do valor de entrada (igual ao valor de saida do map)
     * 3º tipo: tipo da chave de saida
     * 4º tipo: tipo do valor de saida
     */

    public static class ReduceForExe6 extends Reducer<ChaveComposta, DoubleWritable, Text, DoubleWritable> {

        public void reduce(ChaveComposta key, Iterable<DoubleWritable> values, Context con)
                throws IOException, InterruptedException {

            DoubleWritable maior = new DoubleWritable(0);
            for(DoubleWritable o : values){
                if(o.compareTo(maior) > 0){
                    maior = o;
                }
            }

            Text keyText = new Text(key.toString());

            con.write(keyText,maior);
        }
    }
}

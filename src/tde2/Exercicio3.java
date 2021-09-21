package tde2;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Exercicio3 {
    public static void main(String[] args) throws Exception {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        // arquivo de entrada
        Path input = new Path("./in/operacoes.csv");

        // arquivo de saida
        Path output = new Path("./output/exe3.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "countrycount");

        // registro das classes
        j.setJarByClass(Exercicio3.class);//main
        j.setMapperClass(MapForExe3.class);
        j.setReducerClass(ReduceForExe3.class);

        // definicao dos tipos de saida(map e reduce)
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(ChaveComposta.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(ChaveComposta.class);

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
    public static class MapForExe3 extends Mapper<LongWritable, Text, Text, ChaveComposta> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // obtendo conteudo da linha
            String linha = value.toString();

            String[] colunas = linha.split(";");

            String year = colunas[1];

            if(year.equals("2016")) {
                if(NumberUtils.isParsable(colunas[8])) {
                    Text flow = new Text(colunas[4]);

                    ChaveComposta valorSaida = new ChaveComposta(colunas[8], colunas[2]);
                    con.write(flow, valorSaida);
                }
            }
        }
    }

    /**
     * 1º tipo: tipo da chave de entrade (igual a chave de saida do map)
     * 2º tipo: tipo do valor de entrada (igual ao valor de saida do map)
     * 3º tipo: tipo da chave de saida
     * 4º tipo: tipo do valor de saida
     */

    public static class ReduceForExe3 extends Reducer<Text, ChaveComposta, Text, ChaveComposta> {

        public void reduce(Text key, Iterable<ChaveComposta> values, Context con)
                throws IOException, InterruptedException {

            //somando todos os valores de ocorrencia para uma palavra especifica
            DoubleWritable maior = new DoubleWritable(0);
            String code = "";
            for(ChaveComposta o : values){
                DoubleWritable q = new DoubleWritable(Double.parseDouble(o.getKey1()));
                if(q.compareTo(maior) > 0){
                    maior = q;
                    code = o.getKey2();
                }
            }

            con.write(key,new ChaveComposta(maior.toString(), code));
        }
    }
}

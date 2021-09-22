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
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import java.io.IOException;

public class Exercicio4 {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        String[] files = new GenericOptionsParser(c, args).getRemainingArgs();
        // arquivo de entrada
        Path input = new Path("./in/operacoes.csv");

        // arquivo de saida
        Path output = new Path("./output/exe4.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "media");

        // registro das classes
        j.setJarByClass(Exercicio4.class);
        j.setMapperClass(Exercicio4.MapForAverage.class);
        j.setReducerClass(Exercicio4.ReduceForAverage.class);

        //definicao dos tipos de saida
        j.setMapOutputKeyClass(Text.class);
        j.setMapOutputValueClass(CustomValueWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        //cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j,output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapForAverage extends Mapper<LongWritable, Text, Text, CustomValueWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //convertendo a linha em string
            String linha = value.toString();

            //fazendo split para pegar valores
            String[] colunas = linha.split(";");


            if(NumberUtils.isParsable(colunas[5])) {
                //obtendo o preço
                double preco = Double.parseDouble(colunas[5]);

                //ocorrência
                long n = 1;

                Text year = new Text(colunas[1]);

                //passando preço e ocorrência para o sort/shuffle
                con.write(year, new CustomValueWritable(n, preco));
            }

        }
    }

    public static class ReduceForAverage extends Reducer<Text, CustomValueWritable, Text, DoubleWritable> {
        public void reduce(Text key, Iterable<CustomValueWritable> values, Context con)
                throws IOException, InterruptedException {

            // Chega no reduce, uma chave UNICA com uma lista de valres compostos
            // por ocorrência e soma dos preços

            //somando preços e ocorrências
            double somapreco = 0.0;
            long somaNs = 0;

            for(CustomValueWritable o : values){
                somapreco += o.getSoma();
                somaNs += o.getOcorrencia();
            }

            con.write(key, new DoubleWritable(somapreco/somaNs));

        }
    }
}

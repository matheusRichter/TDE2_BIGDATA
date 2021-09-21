package tde2;

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

public class Exercicio5 {
    public static void main(String args[]) throws IOException, ClassNotFoundException, InterruptedException {
        BasicConfigurator.configure();

        Configuration c = new Configuration();
        // arquivo de entrada
        Path input = new Path("./in/operacoes.csv");

        // arquivo de saida
        Path output = new Path("./output/exe5.txt");

        // criacao do job e seu nome
        Job j = new Job(c, "exe5");

        // registro das classes
        j.setJarByClass(Exercicio5.class);
        j.setMapperClass(MapForAverage.class);
        j.setReducerClass(ReduceForAverage.class);

        //definicao dos tipos de saida
        j.setMapOutputKeyClass(ChaveTripla.class);
        j.setMapOutputValueClass(CustomValueWritable.class);
        j.setOutputKeyClass(Text.class);
        j.setOutputValueClass(DoubleWritable.class);

        //cadastro dos arquivos de entrada e saida
        FileInputFormat.addInputPath(j,input);
        FileOutputFormat.setOutputPath(j,output);

        // lanca o job e aguarda sua execucao
        System.exit(j.waitForCompletion(true) ? 0 : 1);
    }


    public static class MapForAverage extends Mapper<LongWritable, Text, ChaveTripla, CustomValueWritable> {

        // Funcao de map
        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {

            //convertendo a linha em string
            String linha = value.toString();

            //fazendo split para pegar valores
            String[] colunas = linha.split(";");

            String flow = colunas[4];
            String country = colunas[0];

            if(flow.equals("Export") && country.equals("Brazil")) {

                //chaves
                String year = colunas[1];
                String category = colunas[9];
                String quantity_name = colunas[7];

                //valor
                double trade_usd = Double.parseDouble(colunas[5]);

                //ocorrencia
                long n = 1;

                //passando temperatura e ocorrencia pro sort/shuffle
                con.write(new ChaveTripla(year, category, quantity_name), new CustomValueWritable(n, trade_usd));
            }
        }
    }

    public static class ReduceForAverage extends Reducer<ChaveTripla, CustomValueWritable, Text, DoubleWritable> {

        public void reduce(ChaveTripla key, Iterable<CustomValueWritable> values, Context con)
                throws IOException, InterruptedException {

            // Chega no reduce, uma chave UNICA com uma lista de valres compostos
            // por ocorrencia e soma dos precos

            //comando precos e ocorrencias
            double somaPrecos = 0.0;
            long somaNs = 0;

            for(CustomValueWritable o : values){
                somaPrecos += o.getSoma();
                somaNs += o.getOcorrencia();
            }

            Text keyText = new Text(key.toString());

            con.write(keyText, new DoubleWritable(somaPrecos/somaNs));

        }
    }
}

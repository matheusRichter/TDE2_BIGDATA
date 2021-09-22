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

    public static class MapForExe3 extends Mapper<LongWritable, Text, Text, ChaveComposta> {

        public void map(LongWritable key, Text value, Context con)
                throws IOException, InterruptedException {
            // obtendo conteudo da linha
            String linha = value.toString();

            // separando as colunas
            String[] colunas = linha.split(";");

            String year = colunas[1]; // armazena o ano da linha em questão
            String quantidade = colunas[8]; // armazena a quantidade da linha em questão
            String codigo = colunas[2]; // armazena o código da commodity da linha em questão

            if(year.equals("2016")) {
                if(NumberUtils.isParsable(quantidade)) {
                    Text flow = new Text(colunas[4]); // armazena o tipo do fluxo

                    ChaveComposta valorSaida = new ChaveComposta(quantidade, codigo);
                    con.write(flow, valorSaida);
                }
            }
        }
    }

    public static class ReduceForExe3 extends Reducer<Text, ChaveComposta, Text, ChaveComposta> {

        public void reduce(Text key, Iterable<ChaveComposta> values, Context con)
                throws IOException, InterruptedException {

            DoubleWritable maior = new DoubleWritable(0); // maior quantidade
            String code = ""; // código da commodity
            for(ChaveComposta o : values){
                // recupera o valor da quantidade que veio do Map
                DoubleWritable q = new DoubleWritable(Double.parseDouble(o.getKey1()));
                if(q.compareTo(maior) > 0){
                    maior = q;
                    code = o.getKey2(); // recupera o código da commodity que veio do Map
                }
            }

            con.write(key,new ChaveComposta(maior.toString(), code));
        }
    }
}

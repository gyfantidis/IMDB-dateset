package exer2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;


public class Exer2 {
    /**
     * Πρώτος Mapper
     */
    public static class CountryMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final IntWritable runtime = new IntWritable();
        private final Text country = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String row = value.toString();
            String[] rowItems = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");

            if (rowItems.length == 9) {

                String strCountry = rowItems[8].replaceAll("\"","");
                String[] coProd = strCountry.split(",");
                for(int i=0; i<coProd.length; i++){
                    try {
                        String countryProd = coProd[i].trim();
                        int minutes=Integer.parseInt(rowItems[3].replaceAll("[^0-9.]", ""));
                        country.set(countryProd);
                        runtime.set(minutes);
                        context.write(country, runtime);
                    } catch (NumberFormatException ex) {
                }
                }

            }
        }
    }

    /**
     * Δευτερος Mapper
     */
    public static class Year_genreMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final IntWritable one = new IntWritable(1);
        private final Text year_genre = new Text();

        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String row = value.toString();
            String[] rowItems = row.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            if (rowItems.length == 9) {

                String genreField = rowItems[4].replaceAll("\"","");
                String[] genre = genreField.split(",");
                for(int i=0; i<genre.length; i++){
                    try {
                        String yearGenre = (rowItems[2] +"_" +genre[i].trim());
                        double ratings=Double.parseDouble(rowItems[6].replaceAll("[^0-9.]", ""));
                        if(ratings>=8) {
                            year_genre.set(yearGenre);
                            context.write(year_genre, one);
                        }
                    } catch (NumberFormatException ex) {
                    }
                }

            }
        }
    }

    /**
     * This is the classic summary reducer which is used in the word count example
     */
    public static class Exer2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
                result.set(sum);
                context.write(key, result);

        }
    }



    public static void main(String[] args) throws Exception {



        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Exer2_countrys");
        job.setJarByClass(Exer2.class);
        job.setMapperClass(Exer2.CountryMapper.class);
        job.setReducerClass(Exer2.Exer2Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        job.waitForCompletion(true);

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Exer2_year_genre");
        job1.setJarByClass(Exer2.class);
        job1.setMapperClass(Exer2.Year_genreMapper.class);
        job1.setReducerClass(Exer2.Exer2Reducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path("input"));
        FileOutputFormat.setOutputPath(job1, new Path("output1"));
        job1.waitForCompletion(true);
    }
}


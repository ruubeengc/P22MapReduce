/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 */
package com.mycompany.p22_mapreduce;

/**
 *
 * @author alumno
 */
import java.io.*;
import java.security.PrivilegedExceptionAction;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.security.UserGroupInformation;

public class P22_MapReduce {

    public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {

        public void map(LongWritable key, Text value, Context context) {
            try {
                String[] str = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))", -2); // --> Regex para no coger las comas que están dentro de unas comillas de nuestro archivo csv.
                // El -2 indica que se incluirán todos los elementos del texto, incluso si hay campos vacíos al final.
                String subject = str[2];

                if (!subject.equals("subject")) {
                    String[] date = str[3].split("-|, "); // --> Con esta regex le decimos que separe por coma o por guiones, ya que había dos formatos de fechas ("dia mes, año" y "dia-mes-año")
                    String year = date[date.length - 1].replace("\"", ""); // Aquí limpiamos el deato del año quitando las comillas del final

                    // Regex para extraer el año de la fecha
                    String regex = "\\d{4}|\\d{2}$"; // Las fechas con el formato de guiones solo aparecen los dos últimos dígitos
                    Pattern pattern = Pattern.compile(regex);

                    // Extracción del año de la fecha y conversión al formato numérico
                    Matcher matcher = pattern.matcher(year);
                    if (matcher.find()) {
                        String yearMatch = matcher.group();
                        if (yearMatch.length() == 2) {
                            yearMatch = "20" + yearMatch; // Para aquellas fechas que solo tienen dos dígitos añadimos el 20 para que luego funcione correctamente el partitioner
                        }
                        int yearInt = Integer.parseInt(yearMatch);
                        context.write(new Text(subject), new IntWritable(yearInt));
                    }
                }
            } catch (Exception e) {
                System.err.println("Excepcion capturada: " + e.getMessage());
                e.printStackTrace(System.err);
            }
        }
    }

    public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            try {
                int sum = 0;
                // Aumentamos el contador de cada categoría según su aparición
                for (IntWritable val : values) {
                    sum += 1;
                }
                context.write(key, new IntWritable(sum));
            } catch (IOException | InterruptedException e) {
                System.err.println("Excepcion capturada: " + e.getMessage());
                e.printStackTrace(System.err);
            }
        }
    }

    public static class CaderPartitioner extends Partitioner<Text, IntWritable> {
        
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            int year = Integer.parseInt(value.toString());
            
            if (numReduceTasks == 0) {
                return 0;
            }
            // "Partimos" los datos según el año de publicación de la noticia:
            if (year <= 2015) {
                return 0;
            } else if (year > 2015 && year <= 2016) {
                return 1 ;
            } else {
                return 2 ;
            }
        }
    }
    
    public static void main(String[] args) {
        // Creamos un menú para que sea el usuario quien decida si quiere usar el programa en el archivo de noticias falsas o en el de noticias verdaderas.
        
        Scanner scan = new Scanner(System.in);
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("a_83026");
        System.out.println("Tiene la posibilidad de analizar unas bases de datos que recogen noticias falsas y ciertas que han aparecido en periódicos de EEUU.");
        boolean salir = false;
        boolean analizadoFakeNews = false;
        boolean analizadoTrueNews = false;

        while (!salir) {
            System.out.println("Indique la opción que desea realizar:");
            System.out.println("1.- Analizar la base de datos de noticias falsas.");
            System.out.println("2.- Analizar la base de datos de noticias verídicas.");
            System.out.println("3.- Salir.");
            int opcion = scan.nextInt();
            switch (opcion) {
                case 1:
                    if (!analizadoFakeNews) {
                        AnalisisFakeNews(ugi);
                        analizadoFakeNews = true;
                    } else {
                        System.out.println("Ya has analizado la base de datos de noticias falsas.");
                    }
                    break;
                case 2:
                    if (!analizadoTrueNews) {
                        AnalisisTrueNews(ugi);
                        analizadoTrueNews = true;
                    } else {
                        System.out.println("Ya has analizado la base de datos de noticias verídicas.");
                    }
                    break;
                case 3:
                    salir = true;
                    break;
                default:
                    System.out.println("¡Opción incorrecta!");
            }

            // Salir del bucle si ambas bases de datos han sido analizadas
            if (analizadoFakeNews && analizadoTrueNews) {
                System.out.println("Ya has analizado los dos archivos, ahora lee los resultados en tu terminal.");
                salir = true;
            }
        }
        scan.close();
    }
    
    private static void AnalisisFakeNews (UserGroupInformation ugi){
        System.out.println("Analizando la base de datos de noticias falsas...");
        try {
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    Configuration conf = new Configuration();
                    conf.set("fs.defaultFS", "hdfs://192.168.10.1:9000");
                    Job job = Job.getInstance(conf, "fakeNewsCount");
                    job.setJarByClass(P22_MapReduce.class);
                    job.setMapperClass(MapClass.class);
                    job.setReducerClass(ReduceClass.class);
                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(IntWritable.class);

                    //set partitioner statement
                    job.setPartitionerClass(CaderPartitioner.class);
                    job.setNumReduceTasks(3);
                    job.setInputFormatClass(TextInputFormat.class);
                    job.setOutputFormatClass(TextOutputFormat.class);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(IntWritable.class);
                    FileInputFormat.addInputPath(job, new Path("/PCD2024/a_83026/DatosNews/FakeNews"));
                    FileOutputFormat.setOutputPath(job, new Path("/PCD2024/a_83026/DatosNews_SalidaFechasParticionadoFake"));
                    boolean finalizado = job.waitForCompletion(true);
                    System.out.println("Finalizado el análisis de Fake.csv: " + finalizado);
                    return null;
                }
            });
        } catch (Exception e) {
            System.err.println("Excepcion capturada: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }
    
    private static void AnalisisTrueNews (UserGroupInformation ugi){
        System.out.println("Analizando la base de datos de noticias verídicas...");
        try {
            ugi.doAs(new PrivilegedExceptionAction<Void>() {
                @Override
                public Void run() throws Exception {
                    Configuration conf = new Configuration();
                    conf.set("fs.defaultFS", "hdfs://192.168.10.1:9000");
                    Job job = Job.getInstance(conf, "trueNewsCount");
                    job.setJarByClass(P22_MapReduce.class);
                    job.setMapperClass(MapClass.class);
                    job.setReducerClass(ReduceClass.class);
                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(IntWritable.class);

                    //set partitioner statement
                    job.setPartitionerClass(CaderPartitioner.class);
                    job.setNumReduceTasks(3);
                    job.setInputFormatClass(TextInputFormat.class);
                    job.setOutputFormatClass(TextOutputFormat.class);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(IntWritable.class);
                    FileInputFormat.addInputPath(job, new Path("/PCD2024/a_83026/DatosNews/TrueNews")); 
                    FileOutputFormat.setOutputPath(job, new Path("/PCD2024/a_83026/DatosNews_SalidaFechasParticionadoTrue"));
                    boolean finalizado = job.waitForCompletion(true);
                    System.out.println("Finalizado el análisis de True.csv: " + finalizado);
                    return null;
                }
            });
        } catch (Exception e) {
            System.err.println("Excepcion capturada: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

}

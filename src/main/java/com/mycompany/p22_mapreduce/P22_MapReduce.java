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
                String[] str = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))", -2); // --> Regex para no coger las comas que están dentro de unas comillas 
                // El -2 indica que se incluirán todos los elementos del texto, incluso si hay campos vacíos al final.
                String subject = str[2];

                if (!subject.equals("subject")) {
                    String[] date = str[3].split("-|, ");
                    String year = date[date.length - 1].replace("\"", "");

                    // Regex para extraer el año de la fecha
                    String regex = "\\d{4}|\\d{2}$";
                    Pattern pattern = Pattern.compile(regex);

                    // Extracción del año de la fecha y conversión al formato numérico
                    Matcher matcher = pattern.matcher(year);
                    if (matcher.find()) {
                        String yearMatch = matcher.group();
                        if (yearMatch.length() == 2) {
                            yearMatch = "20" + yearMatch;
                        }
                        int yearInt = Integer.parseInt(yearMatch);
                        context.write(new Text(subject), new IntWritable(yearInt));
                    }
                }
            } catch (Exception e) {
//                System.err.println(value.toString()); --> Esto era para comprobar y saber donde estaba el error
                e.printStackTrace();
            }
        }
    }

    public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) {

            try {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += 1;

                }
//                System.out.println("La key es: " + key.toString()); // --> Para comprobar si funciona bien 
//                System.out.println("Y su suma es:" + sum);
                context.write(key, new IntWritable(sum)); // --> No escribe todas las keys
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public static class CaderPartitioner extends Partitioner<Text, IntWritable> {

        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            // Entrada partitioner: los datos completos en una colección de pares clave-valor.
            int year = Integer.parseInt(value.toString());
            
            if (numReduceTasks == 0) {
                return 0;
            }
            // "Partimos" los datos según los criterios de edad que se nos idican:
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
// Establecemos las configuraciones correspondientes añadiendo las de los partitioners:
        UserGroupInformation ugi = UserGroupInformation.createRemoteUser("a_83026");
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
                    FileInputFormat.addInputPath(job, new Path("/PCD2024/a_83026/DatosNews"));
                    FileOutputFormat.setOutputPath(job, new Path("/PCD2024/a_83026/DatosNews_SalidaFechasParticionado"));
                    boolean finalizado = job.waitForCompletion(true);
                    System.out.println("Finalizado: " + finalizado);
                    return null;
                }
            });
        } catch (Exception e) {
            System.err.println("Excepcion capturada: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

}

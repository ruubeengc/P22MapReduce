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
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.security.UserGroupInformation;

public class P22_MapReduce {

    public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);

        public void map(LongWritable key, Text value, Context context) {
            try {
                String[] str = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)", -2);
                String subject = str[2];
                if (!("subject".equals(subject))) {
                    context.write(new Text(subject), one);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) {
            try {
                int sum = 0;
                for (IntWritable val : values) {
                    sum += val.get();
                }
                context.write(key, new IntWritable(sum));
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
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
                    Job job = Job.getInstance(conf, "topsal");
                    job.setJarByClass(P22_MapReduce.class);
                    job.setMapperClass(MapClass.class);
                    job.setMapOutputKeyClass(Text.class);
                    job.setMapOutputValueClass(Text.class);

                    //set partitioner statement
//                    job.setPartitionerClass(CaderPartitioner.class);
                    job.setReducerClass(ReduceClass.class);
                    job.setNumReduceTasks(3);
                    job.setInputFormatClass(TextInputFormat.class);
                    job.setOutputFormatClass(TextOutputFormat.class);
                    job.setOutputKeyClass(Text.class);
                    job.setOutputValueClass(Text.class);
                    FileInputFormat.addInputPath(job, new Path("/PCD2024/a_83026/DatosNews"));
                    FileOutputFormat.setOutputPath(job, new Path("/PCD2024/a_83026/DatosNews_Salida6"));
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

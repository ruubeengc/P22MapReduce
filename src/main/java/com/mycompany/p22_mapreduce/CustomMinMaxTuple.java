/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.mycompany.p22_mapreduce;

/**
 *
 * @author alumno
 */
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class CustomMinMaxTuple implements Writable{

    private Double min = Double.valueOf(0);
    private Double max = Double.valueOf(0);
    private long count = 1;

    public Double getMin() {
        return min;
    }

    public void setMin(Double min) {
        this.min = min;
    }

    public Double getMax() {
        return max;
    }

    public void setMax(Double max) {
        this.max = max;
    }

    public long getCount() {
        return count;
    }

    public void setCount(long count) {
        this.count = count;
    }

    public void readFields(DataInput in) throws IOException { // Lee los archivos y los valores que buscamos
        min = in.readDouble();
        max = in.readDouble();
        count = in.readLong();
    }

    public void write(DataOutput out) throws IOException { // Escribe en el archivo los valores
        out.writeDouble(min);
        out.writeDouble(max);
        out.writeLong(count);
    }

    public String toString() {
        return min + "\t" + max + "\t" + count;
    }
}

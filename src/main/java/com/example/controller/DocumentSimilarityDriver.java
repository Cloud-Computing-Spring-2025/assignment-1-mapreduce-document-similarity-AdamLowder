package com.example.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.example.DocumentSimilarityMapper;
import com.example.DocumentSimilarityReducer;

public class DocumentSimilarityDriver {
    
    public static void main(String[] args) throws Exception {
        //create config and job variables
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Document Similarity");
        //set classes for mapper, reducer, and output
        job.setJarByClass(DocumentSimilarityDriver.class);
        job.setMapperClass(DocumentSimilarityMapper.class);
        job.setReducerClass(DocumentSimilarityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        //set the input and output paths
        FileInputFormat.addInputPath(job, new Path(args[0], args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        //run the job
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

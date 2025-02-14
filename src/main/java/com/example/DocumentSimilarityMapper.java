package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;

public class DocumentSimilarityMapper extends Mapper<Object, Text, Text, Text> {
    private Text docPair = new Text();
    private Text word = new Text();
    
    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //retrieve the current document's name
        String fileName = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();
        //document content is the value passed from the input split
        String docContent = value.toString().trim();
        //split document content into words
        String[] wordsArray = docContent.split("\\s+");
        //create a set of words from the document content
        HashSet<String> words = new HashSet<>();
        for (String word : wordsArray){
            words.add(word.trim().toLowerCase());
        }
        //emit pairs of documents for each word
        for (String word : words){
            word.set(word);
            for (String otherDocName : words){
                //only emit pairs of different documents
                if (!otherDocName.equals(fileName)){
                    docPair.set(fileName + "," + otherDocName); 
                    context.write(docPair, word);
                }
            }
        }
    }
}
package com.example;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;

public class DocumentSimilarityMapper extends Mapper<Object, Text, Text, Text> {
    //text variables for docpair and words
    private Text docPair = new Text();
    private Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
        //get the current document's name (file name)
        String fileName = ((org.apache.hadoop.mapreduce.lib.input.FileSplit) context.getInputSplit()).getPath().getName();
        //document content is the value passed from the input split
        String docContent = value.toString().trim();
        //split document content into words
        String[] wordsArray = docContent.split("\\s+");
        //create a set of words from the document
        HashSet<String> words = new HashSet<>();
        for(String wordStr : wordsArray){
            words.add(wordStr.trim().toLowerCase());
        }
        //emit pairs of documents for each word
        for(String wordStr : words){
            word.set(wordStr);
            //emit the document pair for each word shared between documents
            for(String otherDocName : words){
                //only emit pairs of different documents (doc1, doc2)
                if(!otherDocName.equals(fileName)){
                    docPair.set(fileName + "," + otherDocName);
                    context.write(docPair, word);
                }
            }
        }
    }
}
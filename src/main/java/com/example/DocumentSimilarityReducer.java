package com.example;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;

public class DocumentSimilarityReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //split the document pair
        String[] docs = key.toString().split(",");
        String doc1 = docs[0];
        String doc2 = docs[1];
        //create HashSets for intersection and union
        HashSet<String> intersection = new HashSet<>();
        HashSet<String> union = new HashSet<>();
        Iterator<Text> wordIterator = values.iterator();
        //iterate over the words for the document pair
        while (wordIterator.hasNext()){
            String word = wordIterator.next().toString();
            intersection.add(word);
            union.add(word);
        }
        //jaccard Similarity
        double similarity = (double) intersection.size() / union.size();
        //emit the result
        context.write(new Text(doc1 + ", " + doc2), new Text("Similarity: " + similarity));
    }
}

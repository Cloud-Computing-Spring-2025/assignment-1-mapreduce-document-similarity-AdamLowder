Implementation and approach - This project should output the jaccard similarity given input text files. The three classes fucntion as follows:
Driver: Configures the hadoop job and specifies the mapper and reducer classes, taking input data to output into HDFS
Mapper: Processes each input file and ouputs key value pairs, removing duplicates using a hashset
Reducer: Takes the mapper's output, and calculates the jaccard similarity between files, outputting the file pair with the similarity score

Instructions - If the assignment worked as I had planned ou would follow the commands below in the readme file and it should output the jaccard similarity between the input text files but i cant seem to get it working at the moment.

Challenges faced - I did the docker compose step and it seemed to load everything fine, but when i got to the next step it kept on telling me that there was no hadoop master. I also tried to just skip hat and see if the maven step would work but i sarted getting errors and with how busy ive been this week it was too late today to get ta help i suppose. I did finally research the full functionality of the hashset and iterators with the help of online guides such as geeksforgeeks and asking gpt to clarify how some steps should work.

UPDATE: Spoke with the TA and was able o get the run and HDFS functions to work but my code is still a little bit off since we changed a se of arguments in the main function. It prints out in the correct format but we believe it was soemthing that would need a slight change in the mapper and reducer.


[![Open in Visual Studio Code](https://classroom.github.com/assets/open-in-vscode-2e0aaae1b6195c2367325f4f02e2d04e9abb55f0b24a779b69b11b9e10269abc.svg)](https://classroom.github.com/online_ide?assignment_repo_id=18028313&assignment_repo_type=AssignmentRepo)
### **📌 Document Similarity Using Hadoop MapReduce**  

#### **Objective**  
The goal of this assignment is to compute the **Jaccard Similarity** between pairs of documents using **MapReduce in Hadoop**. You will implement a MapReduce job that:  
1. Extracts words from multiple text documents.  
2. Identifies which words appear in multiple documents.  
3. Computes the **Jaccard Similarity** between document pairs.  
4. Outputs document pairs with similarity **above 50%**.  

---

### **📥 Example Input**  

You will be given multiple text documents. Each document will contain several words. Your task is to compute the **Jaccard Similarity** between all pairs of documents based on the set of words they contain.  

#### **Example Documents**  

##### **doc1.txt**  
```
hadoop is a distributed system
```

##### **doc2.txt**  
```
hadoop is used for big data processing
```

##### **doc3.txt**  
```
big data is important for analysis
```

---

# 📏 Jaccard Similarity Calculator

## Overview

The Jaccard Similarity is a statistic used to gauge the similarity and diversity of sample sets. It is defined as the size of the intersection divided by the size of the union of two sets.

## Formula

The Jaccard Similarity between two sets A and B is calculated as:

```
Jaccard Similarity = |A ∩ B| / |A ∪ B|
```

Where:
- `|A ∩ B|` is the number of words common to both documents
- `|A ∪ B|` is the total number of unique words in both documents

## Example Calculation

Consider two documents:
 
**doc1.txt words**: `{hadoop, is, a, distributed, system}`
**doc2.txt words**: `{hadoop, is, used, for, big, data, processing}`

- Common words: `{hadoop, is}`
- Total unique words: `{hadoop, is, a, distributed, system, used, for, big, data, processing}`

Jaccard Similarity calculation:
```
|A ∩ B| = 2 (common words)
|A ∪ B| = 10 (total unique words)

Jaccard Similarity = 2/10 = 0.2 or 20%
```

## Use Cases

Jaccard Similarity is commonly used in:
- Document similarity detection
- Plagiarism checking
- Recommendation systems
- Clustering algorithms

## Implementation Notes

When computing similarity for multiple documents:
- Compare each document pair
- Output pairs with similarity > 50%

### **📤 Expected Output**  

The output should show the Jaccard Similarity between document pairs in the following format:  
```
(doc1, doc2) -> 60%  
(doc2, doc3) -> 50%  
```

---

### **🛠 Environment Setup: Running Hadoop in Docker**  

Since we are using **Docker Compose** to run a Hadoop cluster, follow these steps to set up your environment.  

#### **Step 1: Install Docker & Docker Compose**  
- **Windows**: Install **Docker Desktop** and enable WSL 2 backend.  
- **macOS/Linux**: Install Docker using the official guide: [Docker Installation](https://docs.docker.com/get-docker/)  

#### **Step 2: Start the Hadoop Cluster**  
Navigate to the project directory where `docker-compose.yml` is located and run:  
```sh
docker-compose up -d
```  
This will start the Hadoop NameNode, DataNode, and ResourceManager services.  

#### **Step 3: Access the Hadoop Container**  
Once the cluster is running, enter the **Hadoop master node** container:  
```sh
docker exec -it hadoop-master /bin/bash
```

---

### **📦 Building and Running the MapReduce Job with Maven**  

#### **Step 1: Build the JAR File**  
Ensure Maven is installed, then navigate to your project folder and run:  
```sh
mvn clean package
```  
This will generate a JAR file inside the `target` directory.  

#### **Step 2: Copy the JAR File to the Hadoop Container**  
Move the compiled JAR into the running Hadoop container:  
```sh
docker cp target/similarity.jar hadoop-master:/opt/hadoop-3.2.1/share/hadoop/mapreduce/similarity.jar
```

---

### **📂 Uploading Data to HDFS**  

#### **Step 1: Create an Input Directory in HDFS**  
Inside the Hadoop container, create the directory where input files will be stored:  
```sh
hdfs dfs -mkdir -p /input
```

#### **Step 2: Upload Dataset to HDFS**  
Copy your local dataset into the Hadoop cluster’s HDFS:  
```sh
hdfs dfs -put /path/to/local/input/* /input/
```

---

### **🚀 Running the MapReduce Job**  

Run the Hadoop job using the JAR file inside the container:  
```sh
hadoop jar similarity.jar DocumentSimilarityDriver /input /output_similarity /output_final
```

---

### **📊 Retrieving the Output**  

To view the results stored in HDFS:  
```sh
hdfs dfs -cat /output_final/part-r-00000
```

If you want to download the output to your local machine:  
```sh
hdfs dfs -get /output_final /path/to/local/output
```
---

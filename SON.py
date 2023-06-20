import argparse
import pyspark
from pyspark import SparkContext
import re
import string
CHUNK_SIZE = 5

parser = argparse.ArgumentParser(description='ALL')
parser.add_argument('--c',type=int,default='1',help='Integer that specifies the case: 1 for Case 1, and 2 for Case 2')
parser.add_argument('--s',type=int,default='10',help='Integer that defines the minimum count to qualify as a frequent itemset')
parser.add_argument('--input_file',type=str,default='data/small1.csv',help='path to the data including path, file name and extension')
parser.add_argument('--output_file',type=str,default='hw2/a1t1.txt',help=' path to the output file including path, file name and extension')
args = parser.parse_args()

sc_conf = pyspark.SparkConf() \
    .setAppName('task1') \
    .setMaster('local[*]') \
    .set('spark.driver.memory','8g') \
    .set('spark.executor.memory','4g') 

sc = SparkContext(conf=sc_conf)
sc.setLogLevel("OFF")

# define function to format the data from the csv file
def make_tuple_from_csv(line): 
    output = line.split(',')
    # cast answers to integer, and wrap value in array to allow for concatenation 
    return (int(output[0]),[int(output[1])]) 

 
# Returns all possible subsets of size k from the given list    
def get_subsets(itemset, k):
    itemset = sorted(itemset)
    if k == 0:
        return [[]]
    subsets = []
    for i in range(len(itemset)):
        element = itemset[i]
        rest = itemset[i+1:]
        for subset in get_subsets(rest, k-1):
            subsets.append([element] + subset)
    return subsets


# function to generate a count of candidate itemsets
def generate_candidate_itemsets(chunk,k):
    candidate_itemsets = []

    # iterate through every itemset in the chunk
    for n in range(len(chunk)):
        itemset = chunk[n]
        print("itemset: \n{}\n\n".format(itemset))

        # iterate through every subset of size k:
        subsets_of_size_k = get_subsets(itemset,k)
        print("subsets of size {}: \n{}\n\n".format(k,subsets_of_size_k))
        for subset in subsets_of_size_k : 

            # ignore any subset whose components aren't in the previous frequent_itemset list
            candidate = True
            for component in get_subsets(subset,k-1):
                print("component of the subset: \n{}\n".format(component))
                if component not in frequent_itemsets:
                    candidate = False
                else:
                    print("component found to be frequent.\n\n")
            
            if candidate == True:
                print("candidate found!: \n{}\n\n".format(subset))
                candidate_itemsets.append((tuple(sorted(subset)), 1)) # the key must be sorted, to ensure they combine no matter the order
    return candidate_itemsets

f = open(args.output_file,"w")

# generate RDD, remove duplicate items
data = sc.textFile(args.input_file)\
    .distinct()

# remove header
first_element = data.first()
data = data.filter(lambda x: x != first_element)

# process data into a list of baskets
data = data.map(make_tuple_from_csv)\
    .reduceByKey(lambda a,b: a + b)\

# Split the data into chunks
data_chunks = data.map(lambda x: (x[0] // CHUNK_SIZE, [x[1]]))\
    .reduceByKey(lambda x, y: x + y)

f.write("data: \n{}\n\n".format(data_chunks.collect()))


# count single items using word count algorithm
itemset_counts = data_chunks\
    .flatMap(lambda x: x[1])\
    .flatMap(lambda x: [(item, 1) for item in x])\
    .reduceByKey(lambda x, y: x + y)

f.write("single item counts: \n{}\n\n".format(itemset_counts.collect()))

# Filter out infrequent single items
frequent_itemsets = itemset_counts\
    .filter(lambda x: x[1] >= args.c)\
    .map(lambda x: x[0])\
    .collect()

f.write("frequent itemsets of size 1:\n{}\n\n".format(frequent_itemsets))

k = 2
while(k < 3):

    # at this point, the "frequent_itemsets" value of the previous iteration will be used to generate candidates.

    candidate_itemsets = data_chunks\
        .flatMap(lambda x: generate_candidate_itemsets(x[1],k))\
        .reduceByKey(lambda a, b: a + b)
    
    f.write("Candidate itemsets of size {}:\n{}\n\n".format(k,candidate_itemsets.collect()))
    if(candidate_itemsets.count() == 0):
        break
        

    # find the true frequent pairs:
    frequent_itemsets = candidate_itemsets\
        .filter(lambda x: x[1] >= args.c)\
        .keys()\
        .collect()
    
    if(len(frequent_itemsets) == 0):
        break


    f.write("frequent itemsets of size {}:\n{}\n\n".format(k,frequent_itemsets))
    k += 1

f.close
import argparse
import json
import time
import pyspark
from pyspark.accumulators import AccumulatorParam
import numpy as np

class MatrixAccumulator(AccumulatorParam):
    def zero(self, initialValue):
        return initialValue
    
    def addInPlace(self, matrix, index):
        if type(index) == tuple:
            matrix[index[0]][index[1]] = 1

        else:
            matrix = np.minimum(matrix + index, 1)
        return matrix

def main(input_file, output_file, jac_thr, n_bands, n_rows, sc):

    # step 1: load data into boolean matrix 

    # define a method to load the data from json file into the RDD. 
    def load_data(line):
        data = json.loads(line)
        # only keeps the values relevant to the problem
        return (data["user_id"],data["business_id"])

    # load the data
    reviews = sc.textFile(args.input_file)\
        .map(lambda line: load_data(line))\
        .distinct()
    
    # data is now shaped as follows:
    # Pyspark RDD of reviews. Each review is a tuple as follows: (user_id,business_id)
    
    # grab each distint user and business for processing 
    users = reviews.map(lambda review: review[0]).distinct()
    businesses = reviews.map(lambda review: review[1]).distinct()

    # hash the data 
    user_dict = users.zipWithIndex().collectAsMap()
    business_dict = businesses.zipWithIndex().collectAsMap()

    # broadcast the hash tables. They will be needed by each worker 
    broadcast_user_dict = sc.broadcast(user_dict)
    broadcast_business_dict = sc.broadcast(business_dict)

    # count the data
    num_users = users.count()
    num_businesses = businesses.count()

    # construct boolean matrix. This is an accumulator sharred between the workers
    m = np.zeros(shape=(num_businesses,num_users),dtype=np.dtype('u1'))
    matrix_accumulator = sc.accumulator(m, MatrixAccumulator())

    # define a method to update the accumulator:
    def update_matrix(review):
        # access the broadcased dictionaries 
        user_dict_value = broadcast_user_dict.value
        business_dict_value = broadcast_business_dict.value

        # parse the input
        user_id = review[0]
        business_id = review[1]

        # grab index values from id-strings
        user_index = user_dict_value[user_id]
        business_index = business_dict_value[business_id]

        # update the matrix
        matrix_accumulator.add((business_index, user_index))
    
    reviews.foreach(update_matrix)

    # take the accumulated matrix: 
    matrix = matrix_accumulator.value

    print("this is the shape of the matrix: {}".format(matrix.shape))

    # construct inverse dictionaries to go from index of the matrix to ID
    inverse_user_dict = {value: key for key, value in user_dict.items()}
    inverse_business_dict = {value: key for key, value in business_dict.items()}


    # Step 2: generate the hash functions for minihashing
    num_hash_functions = 4
    hash_functions = []
    for i in range(num_hash_functions):
        # randomly permute the index of the matrix with seeding
        seed = np.random.randint(0, 2**32 - 1)
        permute_index = lambda index, seed=seed: np.random.RandomState(seed).permutation(num_businesses)[index]
        hash_functions.append(permute_index)
    
    index = np.arange(num_businesses)

    signiture_matrix = np.zeros(
        shape=(num_hash_functions,num_users),
        dtype=np.dtype('u1')
    )

    # step 3: generate the signiture matrix
    row = 0
    for hash in hash_functions:
        # construct permuted index
        hashed_index = np.array([hash(i) for i in range(num_businesses)])
        print("hash index: {}".format(hashed_index))

        # identify row of the matrix
        signiture_row = signiture_matrix[row]

        for i in range(num_businesses):
            # go through the rows of the matrix in the permuted order
            permuted_row = matrix[hashed_index[i]]
            # print("permuted row: {}".format(permuted_row))

            # identify any nonzero values, recorde the permuted index where it was found
            for nonzero_index in np.nonzero(permuted_row)[0]:
                if signiture_row[nonzero_index] == 0:
                    signiture_row[nonzero_index] = (i + 1)
            # break if this row of the signiture matrix is complete 
            if np.all(signiture_row != 0):
                break
        row += 1

    print("signiture matrix:\n{}".format(signiture_matrix))

    # step 4: estimate jacard similarity
    file = open(output_file, "w")
    for i in range(num_users):
        for j in range(i + 1,num_users):
            col1 = signiture_matrix[:, i]
            col2 = signiture_matrix[:, j]
            union = signiture_matrix.shape[0]
            intersection = 0
            for n in range(union):
                if col1[n] == col2[n]:
                    intersection += 1
            jaccard = intersection / union

            if(jaccard > jac_thr):
                output_string = "\"u1\": \"{}\", \"u2\": \"{}\", \"sim\": {}".format(inverse_user_dict[i],inverse_user_dict[j],jaccard)
                file.write("{")
                file.write(output_string)
                file.write("}\n")
                
    # for hash in hash_functions:

    file.close 

if __name__ == '__main__':
    start_time = time.time()
    sc_conf = pyspark.SparkConf() \
        .setAppName('hw3_task1') \
        .setMaster('local[*]') \
        .set('spark.driver.memory', '4g') \
        .set('spark.executor.memory', '4g')
    sc = pyspark.SparkContext(conf=sc_conf)
    sc.setLogLevel("OFF")

    parser = argparse.ArgumentParser(description='A1T1')
    parser.add_argument('--input_file', type=str, default='data/review_head.json')
    parser.add_argument('--output_file', type=str, default='outputs/task1.json')
    parser.add_argument('--time_file', type=str, default='outputs/task1.time')
    parser.add_argument('--threshold', type=float, default=0.1)
    parser.add_argument('--n_bands', type=int, default=50)
    parser.add_argument('--n_rows', type=int, default=2)
    args = parser.parse_args()

    main(args.input_file, args.output_file, args.threshold, args.n_bands, args.n_rows, sc)
    sc.stop()

    # log time
    with open(args.time_file, 'w') as outfile:
        json.dump({'time': time.time() - start_time}, outfile)
    print('The run time is: ', (time.time() - start_time))

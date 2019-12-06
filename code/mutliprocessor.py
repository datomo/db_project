from multiprocessing import Pool
import mysql.connector
from mysql.connector import pooling
import math 

global conpool

def f(inputs):
    global conpool
    conpool.get_connection()
    x = [x for x in inputs]
    print(str(x))

if __name__ == '__main__':
    processes = 4
    pool = Pool(processes=processes)
    global conpool
    conpol = mysql.connector.pooling.MySQLConnectionPool(pool_name="pool", pool_size=processes)

    amount = 30
    iter = math.ceil(30/4)

    #for j in range(iter):
    output = []
    for i in range(400):
        output.append([x for x in range(200)])
    pool.map_async(f, output)

    # pool.map(f, output)
    pool.close()
    pool.join()
    print("finished")

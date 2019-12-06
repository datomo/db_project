from multiprocessing import Pool
import math 

def f(inputs):
    x = [x for x in inputs]
    print(str(x))

if __name__ == '__main__':
    pool = Pool(processes=4)

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

from multiprocessing import Pool
import os

def f(x):
    return x*x

def main():
    pool =  Pool(processes=4)
    multiple_results = [pool.apply_async(os.getpid, ()) for i in range(4)]
    print([res.get(timeout=1) for res in multiple_results])
    pool.close()
    pool.join()
    print("closed")

main()
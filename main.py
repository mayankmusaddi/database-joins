from hash_join import *
from sort_join import *
import sys
import time

def run():
    if len(sys.argv) != 5:
        print("Usage: python3 main.py <path of R file> <path of S file> <sort/hash> <M>")
        exit(0)

    filepath_R = sys.argv[1]
    filepath_S = sys.argv[2]
    join_type = sys.argv[3]
    mem_blocks = int(sys.argv[4])

    if join_type == 'hash':
        join = HashJoin(filepath_R, filepath_S, 1, 0, mem_blocks)
    elif join_type == 'sort':
        join = SortJoin(filepath_R, filepath_S, 1, 0, mem_blocks)
    else:
        print("Invalid Join Type Argument. Options <sort/hash>. Aborting")
        exit(0)

    join.run()

if __name__ == '__main__':
    start_time = time.time()
    run()
    end_time = time.time()
    print("Time Taken : ", end_time - start_time)
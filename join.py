import os

class Join:
    def __init__(self, filepath_R, filepath_S, key_R, key_S, mem_blocks, block_size = 100, verbose = True):
        self.relation_R = {
            'filepath' : filepath_R,
            'filename' : os.path.basename(filepath_R),
            'key' : key_R,
            'num_tuples' : self.get_total_tuples(filepath_R)
        }
        self.relation_S = {
            'filepath' : filepath_S,
            'filename' : os.path.basename(filepath_S),
            'key' : key_S,
            'num_tuples' : self.get_total_tuples(filepath_S)
        }
        self.output_filename = self.relation_R['filename'] + '_' + self.relation_S['filename'] + '_join.txt'
        self.mem_blocks = mem_blocks
        self.block_size = block_size
        self.mem_tuples = mem_blocks * block_size
        self.verbose = verbose

    def get_total_tuples(self, filepath):
        if not os.path.isfile(filepath):
            print("Input File Doesn't Exist")
            exit(0)

        count = 0
        with open(filepath, 'r') as f:
            for _ in f:
                count += 1
        return count

    def check_feasible(self):
        pass
    def open_join(self):
        pass
    def getnext(self):
        pass
    def close_join(self):
        pass
    def run(self):
        pass
from join import *

class HashJoin(Join):
    def __init__(self, filepath_R, filepath_S, key_R, key_S, mem_blocks, block_size = 100, verbose = True):
        super().__init__(filepath_R, filepath_S, key_R, key_S, mem_blocks, block_size, verbose)
        self.num_splits = self.mem_blocks - 1

    def check_feasible(self):
        # Min(B(R),B(S)) <= M^2
        if self.mem_blocks < 2 or min(self.relation_R['num_tuples'], self.relation_S['num_tuples']) > self.mem_tuples**2 :
            print("Infeasible memory condition for hash join to work. Aborting")
            exit(0)

    def create_sublists(self, relation):
        fp = []
        for i in range(0, self.num_splits):
            f = open(f'{relation["filename"]}_temp{i}.txt', 'w')
            fp.append(f)

        with open(relation['filepath'], 'r') as f:
            for line in f:
                key = line.split()[ relation['key'] ]
                fp[ hash(key)%self.num_splits ].write(line)

        for i in range(0, self.num_splits):
            fp[i].close()

    def open_join(self):
        self.create_sublists(self.relation_R)
        self.create_sublists(self.relation_S)

    def getnext(self):
        fout = open(self.output_filename, 'w')
        for i in range(0, self.num_splits):
            num_tuple_R = self.get_total_tuples(f'{self.relation_R["filename"]}_temp{i}.txt')
            num_tuple_S = self.get_total_tuples(f'{self.relation_S["filename"]}_temp{i}.txt')

            if min(num_tuple_R, num_tuple_S) >= self.mem_tuples:
                print("Hashed Sublist doesn't fit in main memory. Aborting")
                fout.close()
                os.remove(self.output_filename)
                return

            fR = open(f'{self.relation_R["filename"]}_temp{i}.txt', 'r')
            fS = open(f'{self.relation_S["filename"]}_temp{i}.txt', 'r')
            
            if num_tuple_R <= num_tuple_S :
                f_min = (fR, self.relation_R['key'])
                f_max = (fS, self.relation_S['key'])
            else:
                f_min = (fS, self.relation_S['key'])
                f_max = (fR, self.relation_R['key'])

            lines_min = f_min[0].readlines()
            for line_max in f_max[0]:
                for line_min in lines_min:
                    if line_max.split()[f_max[1]] == line_min.split()[f_min[1]]:
                        line = ""
                        if f_max[1] > f_min[1]:
                            line += line_max.split()[0] + " " + line_min
                        else:
                            line += line_min.split()[0] + " " + line_max
                        fout.write(line)
            fR.close()
            fS.close()
        fout.close()

    def close_join(self):
        for i in range(0, self.num_splits):
            os.remove(f'{self.relation_R["filename"]}_temp{i}.txt')
            os.remove(f'{self.relation_S["filename"]}_temp{i}.txt')

    def run(self):
        self.check_feasible()
        self.open_join()
        self.getnext()
        self.close_join()
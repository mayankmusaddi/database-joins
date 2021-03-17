from join import *
import math
import heapq

class Tuple(object):
    def __init__(self, val, column, fp = -1):
        self.val = val
        self.fp = fp
        self.column = column
    
    def __lt__(self, other):
        return (self.val[self.column] < other.val[self.column])

class SortJoin(Join):
    def __init__(self, filepath_R, filepath_S, key_R, key_S, mem_blocks, block_size = 100, verbose = True):
        super().__init__(filepath_R, filepath_S, key_R, key_S, mem_blocks, block_size, verbose)
        if self.mem_tuples > 0:
            self.relation_R['num_splits'] = math.ceil(self.relation_R['num_tuples']/self.mem_tuples)
            self.relation_S['num_splits'] = math.ceil(self.relation_S['num_tuples']/self.mem_tuples)
        self.relation_R['heap'] = []
        self.relation_S['heap'] = []
        self.relation_R['fp'] = []
        self.relation_S['fp'] = []

    def sort_sublist(self, relation, lines, j):
        lines.sort()
        with open(f'{relation["filename"]}_temp{j}.txt', 'w') as w:
            for line in lines:
                w.write(' '.join(line.val)+"\n")

    def create_sublists(self, relation):
        relation['filepath'], self.mem_tuples, relation['key']
        lines = []
        fn = 0
        with open(relation['filepath'], 'r') as f:
            for i,line in enumerate(f):
                lines.append(Tuple(line.split(), relation['key']))
                if (i+1) % self.mem_tuples == 0:
                    self.sort_sublist(relation, lines, fn)
                    fn += 1
                    lines = []
            if len(lines) != 0:
                self.sort_sublist(relation, lines, fn)

    def init_heap(self, relation):
        fp = []
        for i in range(0, relation['num_splits']):
            f = open(f'{relation["filename"]}_temp{i}.txt', 'r')
            fp.append(f)

        h = []
        for i in range(0, relation['num_splits']):
            line = fp[i].readline().split()
            line = Tuple(line, relation['key'], i)
            heapq.heappush(h, line)

        relation['fp'] = fp
        relation['heap'] = h

    def getnext_min(self, relation):
        if len(relation['heap']) <= 0:
            return ""

        s = heapq.heappop(relation['heap'])
        min_line = ' '.join(s.val)+"\n"
        i = s.fp
        line = relation['fp'][i].readline().split()
        if line != []:
            line = Tuple(line, relation['key'], i)
            heapq.heappush(relation['heap'], line)

        return min_line

    def check_feasible(self):
        # B(R) + B(S) <= M^2
        if self.relation_R['num_tuples'] + self.relation_S['num_tuples'] > self.mem_tuples**2 :
            print("Infeasible memory condition for sort join to work. Aborting")
            exit(0)

    def open_join(self):
        self.create_sublists(self.relation_R)
        self.create_sublists(self.relation_S)

    def getnext(self):
        fout = open(self.output_filename, 'w')
        
        self.init_heap(self.relation_R)
        self.init_heap(self.relation_S)

        line_R = self.getnext_min(self.relation_R).split()
        line_S = self.getnext_min(self.relation_S).split()

        end_flag = False
        while True:
            if line_R == [] or line_S == []:
                break

            key_R = line_R[self.relation_R['key']]
            key_S = line_S[self.relation_S['key']]

            while key_R < key_S:
                line_R = self.getnext_min(self.relation_R).split()
                if line_R == []:
                    end_flag = True
                    break
                key_R = line_R[self.relation_R['key']]

            while key_S < key_R:
                line_S = self.getnext_min(self.relation_S).split()
                if line_S == []:
                    end_flag = True
                    break
                key_S = line_S[self.relation_S['key']]

            if end_flag:
                break

            buffer_R = []
            while line_R != [] and line_R[self.relation_R['key']] == key_R:
                buffer_R.append(line_R)
                line_R = self.getnext_min(self.relation_R).split()

            buffer_S = []
            while line_S != [] and line_S[self.relation_S['key']] == key_S:
                buffer_S.append(line_S)
                line_S = self.getnext_min(self.relation_S).split()

            for tup_R in buffer_R:
                for tup_S in buffer_S:
                    fout.write(' '.join(tup_R)+' '+tup_S[1]+"\n")

        fout.close()

    def close_join(self):
        for i in range(0, self.relation_R['num_splits']):
            self.relation_R['fp'][i].close()
        for i in range(0, self.relation_S['num_splits']):
            self.relation_S['fp'][i].close()

        for i in range(0, self.relation_R['num_splits']):
            os.remove(f'{self.relation_R["filename"]}_temp{i}.txt')
        for i in range(0, self.relation_S['num_splits']):
            os.remove(f'{self.relation_S["filename"]}_temp{i}.txt')

    def run(self):
        self.check_feasible()
        self.open_join()
        self.getnext()
        self.close_join()
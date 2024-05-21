from mrjob.job import MRJob

class Odd(MRJob):
    def mapper(self, _, line):
        row = line.split()
        for num in row:
            num = int(num)
            if num % 2 == 0:
                yield "even", num
            else:
                yield "odd", num

    def reducer(self, key, values):
        tot = 0
        cnt = 0
        for v in values:
            tot += v
            cnt += 1
        yield "the sum is: " + str(tot) + " and the freq is:", cnt

if __name__ == '__main__':
    Odd.run()

from mrjob.job import MRJob

class OddEven(MRJob):
    def mapper(self, _, line):
        row = line.split()
        for i in row:
            try:
                num = int(i)
                if num % 2:
                    yield "odd", (1, num)
                else:
                    yield "even", (1, num)
            except ValueError:
                pass

    def reducer(self, key, values):
        values_list = list(values)
        cnt = sum(i[0] for i in values_list)
        total = sum(i[1] for i in values_list)
        yield "Sum of " + key + " numbers is :",str(total)
        yield "Count of " + key + " numbers is: ", str(cnt)

if __name__ == '__main__':
    OddEven.run()

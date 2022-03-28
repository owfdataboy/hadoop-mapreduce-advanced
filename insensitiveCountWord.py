from mrjob.job import MRJob
from mrjob.step import MRStep

class insensitiveCountWord(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_word_count),
            MRStep(reducer=self.reducer_word_count)
        ]
        
    def mapper_word_count(self, _, line):
        for word in line.split():
            yield(word.lower(), 1)

    def reducer_word_count(self, word, counts):
        yield None,(sum(counts), word)


if __name__=='__main__':
    insensitiveCountWord.run()

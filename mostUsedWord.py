from mrjob.job import MRJob
from mrjob.step import MRStep

class mostUsedWord(MRJob):

    def steps(self):
        return [
            MRStep(mapper=self.mapper_word_count,
            reducer=self.reducer_word_count),
            MRStep( reducer=self.reducer_find_max_word)
        ]
        
    def mapper_word_count(self, _, line):
        for word in line.split():
            yield(word.lower(), 1)
    def reducer_word_count(self, word, counts):
        yield None,(sum(counts), word)

    def reducer_find_max_word(self,_,word_count_pairs):
        yield max(word_count_pairs)

if __name__=='__main__':
    mostUsedWord.run()

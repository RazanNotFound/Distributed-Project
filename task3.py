from mrjob.job import MRJob
from mrjob.step import MRStep

class TopThreeGrades(MRJob):

  #mapper function splits the code at every comma
    def mapper(self, _, line):
        fields = line.split(",")
        year = int(fields[4]) #5th column, so fields[4]
        grade = int(fields[2]) #3rd column, so fields[2]
        yield year, grade #outputs (year, grade) as key-value pairs
  
  #reducer function and sort them and reverse to get the highest first and pick the top 3 
    def reducer(self, year, grades):
        top_grades = sorted(grades, reverse=True)[:3]   #:3333 hehe
        yield year, top_grades #pairs of year and grade

if __name__ == "__main__":
    TopThreeGrades.run()

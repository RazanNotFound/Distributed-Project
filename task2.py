from mrjob.job import MRJob
class UniversityAverageGrade(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")
        uni_name = fields[3]
        grade = int(fields[2])
        yield uni_name, grade

    def reducer(self, uni_name, grades):
        grades_list = list(grades)
        avg_grade = sum(grades_list) / len(grades_list)
        yield uni_name, avg_grade

if __name__ == "__main__":
    UniversityAverageGrade.run()
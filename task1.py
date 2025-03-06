from mrjob.job import MRJob
class CourseAverageGrade(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")
        course_name = fields[1]
        grade = int(fields[2])
        yield course_name, grade

    def reducer(self, course_name, grades):
        grades_list = list(grades)
        avg_grade = sum(grades_list) / len(grades_list)
        yield course_name, avg_grade

if __name__ == "__main__":
    CourseAverageGrade.run()

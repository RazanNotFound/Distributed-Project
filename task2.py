from mrjob.job import MRJob
class UniversityAverageGrade(MRJob):

# The mapper processes each line of input data.
    def mapper(self, _, line):
        # Split the input at every comma
        fields = line.split(",")

        # Extract the university name (4th column)
        uni_name = fields[3]

        # Extract the grade (3rd column) and convert to integer
        grade = int(fields[2])

        # return a key-value pair: (university name, grade)
        yield uni_name, grade

    # The reducer receives all grades grouped by university name.
    def reducer(self, uni_name, grades):
        # Convert the grades (which is a generator) to a list to allow multiple uses.
        grades_list = list(grades)

        # Calculate the average grade.
        avg_grade = sum(grades_list) / len(grades_list)

        # return key-value pair the university name and its average grade.
        yield uni_name, avg_grade

if __name__ == "__main__":
    UniversityAverageGrade.run()
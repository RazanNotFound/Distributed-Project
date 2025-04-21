# Importing the base class MRJob from the mrjob library
from mrjob.job import MRJob

# Define a class that inherits from MRJob to perform MapReduce
class CourseAverageGrade(MRJob):

    # The mapper function processes each line of input
    def mapper(self, _, line):
        # Split the line by commas into a list of fields
        fields = line.split(",")
        
        # Extract the course name from the second field
        course_name = fields[1]
        
        # Extract the grade from the third field and convert it to an integer
        grade = int(fields[2])
        
        # return key-value pairs of the course name as the key, and the grade as the value
        yield course_name, grade

    # The reducer function receives all grades grouped by course_name
    def reducer(self, course_name, grades):
        # Convert the iterable of grades into a list so we can reuse it
        grades_list = list(grades)
        
        # Calculate the average grade
        avg_grade = sum(grades_list) / len(grades_list)
        
        # return key-value pairs of the course name and the computed average grade
        yield course_name, avg_grade

# run script as main program
if __name__ == "__main__":
    CourseAverageGrade.run()

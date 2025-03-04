# Distributed-Project

- This assignment explores the MapReduce methodology for analyzing student grades using the ”UniversityCoursesGrades” dataset. The dataset is provided in the file coursegrades.txt, where each line follows the format:
  ####  (Year, Course Name, Student Grade, University Name)
- Each value is separated by a comma.

1. [ ] Task 1: Average Grade per Course
  Find the average grade for each course and determine which course has the highest average.
  - Mapper:
    - Generates key-value pairs: (Course Name, Grade) e.g., (Distributed, 100)
  - Reducer:
    - Receives (Course Name, (G1, G2, ...)), e.g., (Distributed, (100, 55, 85))
    - Computes the average grade for each course
    - Outputs: (Course Name, Average Grade), e.g., (Distributed, 80)
2. [ ] Task 2: Average Grade per University
  Find the average grade for each university and determine which university has the highest average.
  - Mapper:
    - Generates key-value pairs: (University Name, Grade) e.g., (GIU, 200), (GIU, 60)
  - Reducer:
    - Receives (University Name, (G1, G2, ...)), e.g., (GIU, (200, 60, 70))
    - Computes the average grade for each university
    - Outputs: (University Name, Average Grade), e.g., (GIU, 110)
3. [ ] Bonus Task: Top 3 Highest Grades per Year
  Identify the top 3 highest student grades recorded for each academic year.
  - Mapper:
    - Generates key-value pairs: (Year, Grade) e.g., (2025, 80)
  - Reducer:
    - Receives (Year, [G1, G2, ...]), e.g., (2025, [80, 60, 50, 100, ...])
    - Sorts the grades in descending order
    - Extracts the top 3 highest grades
    - Outputs: (Year, [Top 3 Grades]), e.g., (2025, [100, 80, 60])

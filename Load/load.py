import pandas as pd
import pyodbc
from Transform.normalise import Normalise


class Load(Normalise):

    def __init__(self):
        super().__init__()

    def loading_data_to_sql(self):
        server = 'localhost,1433'
        database = 'Sparta_Final_Project'
        username = 'SA'
        password = 'Kendrick2018<3'

        cnxn = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=' + server +
            ';DATABASE=' + database +
            ';UID=' + username +
            ';PWD=' + password)
        cursor = cnxn.cursor()

        # Creates a table called Iris and drops it if exists
        # we could potentially create a new database using an existing database



        cursor.execute("""
                DROP TABLE IF EXISTS trainer
                CREATE TABLE trainer (
                    trainer_id INT PRIMARY KEY,
                    forename VARCHAR(255),
                    lastname VARCHAR(255)
                );
                """)

        for index, row in self.trainers_table().iterrows():
            cursor.execute("INSERT INTO trainer (trainer_id, forename, lastname) VALUES (?, ?, ?)",
                           (row['Trainer_ID'], row['Trainer_Forename'], row['Trainer_Lastname']))


        create_table_query = """
            DROP TABLE IF EXISTS weaknesses
            CREATE TABLE weaknesses (
                weakness_id INT PRIMARY KEY,
                weakness VARCHAR(MAX)
            );
        """

        # Execute the SQL command to create the table
        cursor.execute(create_table_query)

        # Commit changes
        cnxn.commit()

        # Insert Dataframe into SQL Server:
        for index, row in self.weakness_table().iterrows():
            cursor.execute("INSERT INTO weaknesses (weakness_id, weakness) VALUES (?, ?)",
                           row['Weaknesses_ID'], row['Weaknesses'])



        cursor.execute(
            """
             DROP TABLE IF EXISTS strengths;

             CREATE Table strengths (
                 strength_id INT PRIMARY KEY,
                 strength VARCHAR(MAX)
             );

             """
        )

        # Insert Dataframe into SQL Server:
        for index, row in self.strength_table().iterrows():
            cursor.execute("INSERT INTO strengths (strength_id, strength) values(?,?)",
                           row['Strengths_ID'], row['Strengths'])

        cursor.execute(
            """
             DROP TABLE IF EXISTS metrics;

             CREATE Table metrics (
                 metric_id INT PRIMARY KEY,
                 metric VARCHAR(MAX)
             );

             """
        )

        # Insert Dataframe into SQL Server:
        for index, row in self.metrics_table().iterrows():
            cursor.execute("INSERT INTO metrics (metric_id,metric) values(?,?)",
                           row['metric_id'], row['metric'])

        cursor.execute(
            """
             DROP TABLE IF EXISTS weeks;

             CREATE Table weeks (
                 week_id INT PRIMARY KEY,
                 week_number VARCHAR(3)
             );

             """
        )

        # Insert Dataframe into SQL Server:
        for index, row in self.weeks_table().iterrows():
            cursor.execute("INSERT INTO weeks (week_id,week_number) values(?,?)",
                           row['week_id'], row['week_number'])

        cursor.execute(
            """
            DROP TABLE IF EXISTS University;

            CREATE Table University (
                university_id INT PRIMARY KEY,
                university VARCHAR(MAX)
            );
            """
        )

        for index, row in self.university_table().iterrows():
            cursor.execute("INSERT INTO University (university_id, university) VALUES (?, ?)",
                           row['University_ID'], row['Uni'])

        cursor.execute(
            """
            DROP TABLE IF EXISTS University_grade;

            CREATE Table University_grade (
                grade_id INT PRIMARY KEY,
                grade VARCHAR(MAX)
            );
            """
        )

        for index, row in self.grade_table().iterrows():
            cursor.execute("INSERT INTO University_grade (grade_id, grade) VALUES (?, ?)",
                           row['Grade_ID'], row['Degree'])

        cursor.execute(
            """
            DROP TABLE IF EXISTS Tech;

            CREATE Table Tech (
                tech_id INT PRIMARY KEY,
                tech VARCHAR(MAX)
            );
            """
        )

        for index, row in self.tech_table().iterrows():
            cursor.execute("INSERT INTO Tech (tech_id, tech) VALUES (?, ?)",
                           row['Language_ID'], row['Language'])

        cursor.execute("""
                DROP TABLE IF EXISTS city;

                CREATE TABLE city (
                    city_id INT PRIMARY KEY,
                    city VARCHAR(255)
                );
                """)

        for index, row in self.city_table().iterrows():
            cursor.execute("INSERT INTO city (city_id, city) VALUES (?, ?)",
                           (row['City_ID'], row['City']))

        cursor.execute("""
                DROP TABLE IF EXISTS postcode;

                CREATE TABLE postcode (
                    postcode_id INT PRIMARY KEY,
                    postcode VARCHAR(255)
                );
                """)

        for index, row in self.postcode_table().iterrows():
            cursor.execute("INSERT INTO postcode (postcode_id, postcode) VALUES (?, ?)",
                           (row['Postcode_ID'], row['Postcode']))

        cursor.execute("""
                DROP TABLE IF EXISTS talent_team;

                CREATE TABLE talent_team (
                    talent_team_id INT PRIMARY KEY,
                    forename VARCHAR(255),
                    lastname VARCHAR(255)
                );
                """)

        for index, row in self.talent_team().iterrows():
            cursor.execute("INSERT INTO talent_team (talent_team_id, forename, lastname) VALUES (?, ?, ?)",
                           (row['Talent_Team_ID'], row['Talent_Forename'], row['Talent_Lastname']))

        cursor.execute("""
                DROP TABLE IF EXISTS gender;

                CREATE TABLE gender (
                    gender_id INT PRIMARY KEY,
                    gender VARCHAR(255)
                );
                """)

        for index, row in self.gender_table().iterrows():
            cursor.execute("INSERT INTO gender (gender_id, gender) VALUES (?, ?)",
                           (row['Gender_ID'], row['Gender']))

        # Execute the SQL command to create the table
        cursor.execute(create_table_query)

        # Commit changes
        cnxn.commit()

        cursor.execute(

            """
            DROP TABLE IF EXISTS cour;

            CREATE Table cour (
                course_id INT PRIMARY KEY,
                course_interest VARCHAR(MAX)
            );
            """
        )

        for index, row in self.cour_table().iterrows():
            cursor.execute("INSERT INTO cour (course_id, course_interest) VALUES (?, ?)",
                           row['Course_ID'], row['Course_Interest'])

        create_table_query = """
                    DROP TABLE IF EXISTS courses;

                    CREATE TABLE courses (
                        coursetrainer_id INT PRIMARY KEY,
                        course_id INT,
                        trainer_id INT,
                        CONSTRAINT FK_courses_trainer FOREIGN KEY (trainer_id) REFERENCES trainer(trainer_id)
                    );
                """
        cursor.execute(create_table_query)

        #Insert Dataframe into SQL Server:
        for index, row in self.courses_table().iterrows():
            cursor.execute("INSERT INTO courses (coursetrainer_id,course_id, trainer_id) VALUES (?, ?, ?)",
                           (int(row["CoursesTrainer_ID"]),int(row['Course_ID']),int(row['Trainer_ID'])))


        create_table_query = """
                    DROP TABLE IF EXISTS student_weaknesses;

                    CREATE TABLE student_weaknesses (
                        student_id INT,
                        weakness_id INT PRIMARY KEY,
                        CONSTRAINT FK_studentweakness_weakness FOREIGN KEY (weakness_id) REFERENCES weaknesses(weakness_id)
                    );
                """

        # Execute the SQL command to create the table
        cursor.execute(create_table_query)

        # Insert DataFrame data into the created SQL table (student_weaknesses)
        for index, row in self.weakness_junction_df().iterrows():
            cursor.execute("""
                        INSERT INTO student_weaknesses (
                            student_id, weakness_id
                        ) VALUES (?, ?)
                    """,
                           row['Student_ID'], row['Weaknesses_ID'])

        cursor.execute(
            """
             DROP TABLE IF EXISTS student_strengths;

             CREATE Table student_strengths (
                 student_id INT,
                 strength_id INT PRIMARY KEY,
                 CONSTRAINT FK_studentstrength_strength FOREIGN KEY (strength_id) REFERENCES strengths(strength_id)
             );

             """
        )

        # Insert Dataframe into SQL Server:
        for index, row in self.strength_junction_table().iterrows():
            cursor.execute("INSERT INTO student_strengths (student_id, strength_id) values(?,?)",
                           row['Student_ID'], row['Strengths_ID'])

        cursor.execute(
            """
            DROP TABLE IF EXISTS Education;

            CREATE Table Education (
                education_id INT PRIMARY KEY,
                student_id INT,
                university_id INT,
                grade_id INT,
                CONSTRAINT FK_education_university FOREIGN KEY (university_id) REFERENCES university(university_id),
                CONSTRAINT FK_education_universitygrade FOREIGN KEY (grade_id) REFERENCES university_grade(grade_id)
            );
            """
        )
        # Insert Dataframe into SQL Server:
        for index, row in self.education().iterrows():
            cursor.execute("INSERT INTO Education (education_id, student_id, university_id ,grade_id) values(?,?,?,?)",
                           int(row["Education_ID"]),int(row['Student_ID']),int(row['University_ID']), int(row['Grade_ID']))

        cursor.execute(
            """
                DROP TABLE IF EXISTS address;

                CREATE TABLE address (
                    addressstudent_id INT PRIMARY KEY,
                    address_id INT ,
                    student_id INT,
                    city_id INT,
                    postcode_id INT,
                    CONSTRAINT FK_address_city FOREIGN KEY (city_id) REFERENCES city(city_id),
                    CONSTRAINT FK_address_postcode FOREIGN KEY (postcode_id) REFERENCES postcode(postcode_id)
                );
                """)

        for index, row in self.address_table().iterrows():
            cursor.execute("INSERT INTO address (addressstudent_id,address_id, student_id, city_id, postcode_id) VALUES (?, ?, ?, ?, ?)",
                           (row["AddressStudent_ID"],row['Address_ID'], row['Student_ID'], row['City_ID'], row['Postcode_ID']))

        cursor.execute(
            """
            DROP TABLE IF EXISTS pre_course;

            CREATE TABLE pre_course (
                precourse_id INT PRIMARY KEY,
                psychometric_score INT,
                presentation_score INT,
                course_interest VARCHAR(MAX),
                result VARCHAR(MAX),
                sparta_day_date DATE,
                application_date DATE,
            );
        """)
        # CONSTRAINT
        # FK_precourse_student
        # FOREIGN
        # KEY(student_id)
        # REFERENCES
        # student(student_id),

        # Execute the SQL command to create the table
        cursor.execute(create_table_query)
        df = self.precourses()
        for index, row in df.iterrows():
            cursor.execute(
                "INSERT INTO pre_course (precourse_id, psychometric_score, presentation_score, course_interest, result, sparta_day_date, application_date) VALUES (?, ?, ?, ?, ?, ?, ?)",
                (row["Precourse_ID"], row['Psychometrics(%)'], row['Presentation(%)'],
                 row['Course_Interest'], row['Result'], row['Sparta_Day_Date'],
                 row['Application_Date']))





        cursor.execute(
            """
            DROP TABLE IF EXISTS student;

            CREATE TABLE student (
            student_id INT PRIMARY KEY,
            forename VARCHAR(255),
            lastname VARCHAR(255),
            dob DATE,
            gender_id INT,
            email VARCHAR(255),
            addressstudent_id INT,
            phone_number VARCHAR(50),
            precourse_id INT,
            self_development VARCHAR(3),
            geo_flex VARCHAR(3),
            financial_support VARCHAR(3),
            coursetrainer_id INT,
            talent_team_id INT,
            start_date DATE,
            CONSTRAINT FK_student_gender FOREIGN KEY (gender_id) REFERENCES gender(gender_id),
            CONSTRAINT FK_student_course FOREIGN KEY (coursetrainer_id) REFERENCES courses(coursetrainer_id),
            CONSTRAINT FK_student_talent FOREIGN KEY (talent_team_id) REFERENCES talent_team(talent_team_id),
            CONSTRAINT FK_student_address FOREIGN KEY (addressstudent_id) REFERENCES address(addressstudent_id),
            CONSTRAINT FK_student_precourse FOREIGN KEY (precourse_id) REFERENCES pre_course(precourse_id)        
        );

             """
        )

        for index, row in self.students().iterrows():
            cursor.execute(
                "INSERT INTO student (student_id, forename, lastname, dob, gender_ID, email, addressstudent_id,"
                " phone_number, precourse_id, self_development, geo_flex, financial_support, coursetrainer_id, "
                "talent_team_id, start_date) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    row['Student_ID'],
                    row['Forename'],
                    row['Lastname'],
                    row['Dob'],  # Format date as string
                    row['Gender_ID'],
                    row['Email'],
                    row['AddressStudent_ID'],
                    row['Phone_Number'],
                    row["Precourse_ID"],
                    row['Self_Development'],
                    row['Geo_Flex'],
                    row['Financial_Support_Self'],
                    row['CoursesTrainer_ID'],
                    row['Talent_Team_ID'],
                    row['Start_Date'] # Format date as string
                )
            )

            cursor.execute(
                """
                 DROP TABLE IF EXISTS weekly_scores;

                 CREATE Table weekly_scores (
                     weekly_score_id INT PRIMARY KEY,
                     student_id INT,
                     metric_id INT,
                     week_id INT,
                     score INT,
                     CONSTRAINT FK_weeklyscore_metric FOREIGN KEY (metric_id) REFERENCES metrics(metric_id),
                     CONSTRAINT FK_weeklyscore_weeks FOREIGN KEY (week_id) REFERENCES weeks(week_id)
                 );

                 """
            )


            # Insert Dataframe into SQL Server:
            for index, row in self.weekly_scores_table().iterrows():
                cursor.execute(
                    "INSERT INTO weekly_scores (weekly_score_id,student_id,metric_id,week_id,score) values(?,?,?,?,?)",
                    row["Weekly_Score_ID"], row["Student_ID"],row['Metric_ID'], row['Week_ID'], row['Score'])

            cursor.execute(

                """
                DROP TABLE IF EXISTS Tech_self_score;

                CREATE Table Tech_self_score (
                    tech_score_id INT PRIMARY KEY,
                    student_id INT,
                    tech_id INT,
                    score INT,
                    CONSTRAINT FK_techscore_tech FOREIGN KEY (tech_id) REFERENCES tech(tech_id)
                );

                """
            )

            for index, row in self.tech_score_table().iterrows():
                cursor.execute("INSERT INTO Tech_self_score (tech_score_id,student_id,tech_id, score) VALUES (?,?, ?, ?)",
                               row["Tech_Score_ID"],row["Student_ID"], row['Language_ID'], row['Score'])

        cnxn.commit()
        cursor.close()
        cnxn.close()


instance1 = Load()
instance1.loading_data_to_sql()
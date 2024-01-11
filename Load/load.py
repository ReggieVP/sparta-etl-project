import pandas as pd
import pyodbc


class Load:

    def loading_data_to_sql(self):
        server = 'localhost,1433'
        database = 'Sparta_Final_Project'
        username = 'SA'
        password = 'Coc123-4wegfbdhhdsT*5'

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
                CREATE TABLE trainer (
                    trainer_id INT,
                    forename VARCHAR(255),
                    lastname VARCHAR(255)
                );
                """)

        for index, row in trainer.iterrows():
            cursor.execute("INSERT INTO trainer (trainer_id, forename, lastname) VALUES (?, ?, ?)",
                           (row['Trainer_ID'], row['Trainer_Forename'], row['Trainer_Lastname']))


        create_table_query = """
            CREATE TABLE weaknesses (
                weakness_id INT,
                weakness VARCHAR(MAX)
            );
        """

        # Execute the SQL command to create the table
        cursor.execute(create_table_query)

        # Commit changes
        conn.commit()

        # Insert Dataframe into SQL Server:
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO weaknesses (weakness_id, weakness) VALUES (?, ?)",
                           row['Weakness_ID'], row['Weakness'])



        cursor.execute(
            """
             DROP TABLE IF EXISTS strengths;

             CREATE Table strengths (
                 strength_id INTEGER ,
                 strength VARCHAR
             );

             """
        )

        # Insert Dataframe into SQL Server:
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO strengths (strength_id, strength) values(?,?)",
                           row['Strength_ID'], row['Strength'])

        cursor.execute(
            """
             DROP TABLE IF EXISTS metrics;

             CREATE Table metrics (
                 metric_id INTEGER,
                 metric VARCHAR 
             );

             """
        )

        # Insert Dataframe into SQL Server:
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO metrics (metric_id,metric) values(?,?)",
                           row['Metric_ID'], row['Metric'])

        cursor.execute(
            """
             DROP TABLE IF EXISTS weeks;

             CREATE Table weeks (
                 week_id INT,
                 week_number INT
             );

             """
        )

        # Insert Dataframe into SQL Server:
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO weeks (week_id,week_number) values(?,?)",
                           row['Week_ID'], row['Week_Number'])

        cursor.execute(
            """
            DROP TABLE IF EXISTS University;

            CREATE Table University (
                university_id INT,
                university VARCHAR(MAX)
            );
            """
        )

        for index, row in University.iterrows():
            cursor.execute("INSERT INTO University (university_id, university) VALUES (?, ?)",
                           row['University_ID'], row['University'])

        cursor.execute(
            """
            DROP TABLE IF EXISTS University_grade;

            CREATE Table University_grade (
                grade_id INT,
                grade VARCHAR(MAX)
            );
            """
        )

        for index, row in University_grade.iterrows():
            cursor.execute("INSERT INTO University_grade (grade_id, grade) VALUES (?, ?)",
                           row['Grade_ID'], row['Grade'])

        cursor.execute(
            """
            DROP TABLE IF EXISTS Tech;

            CREATE Table Tech (
                tech_id INT,
                tech VARCHAR(MAX)
            );
            """
        )

        for index, row in Tech.iterrows():
            cursor.execute("INSERT INTO Tech (tech_id, tech) VALUES (?, ?)",
                           row['Tech_ID'], row['Tech'])

        cursor.execute("""
                CREATE TABLE city (
                    city_id INT,
                    city VARCHAR(255)
                );
                """)

        for index, row in city.iterrows():
            cursor.execute("INSERT INTO city (city_id, city) VALUES (?, ?)",
                           (row['City_ID'], row['City']))

        cursor.execute("""
                CREATE TABLE postcode (
                    postcode_id INT,
                    postcode VARCHAR(255)
                );
                """)

        for index, row in postcode.iterrows():
            cursor.execute("INSERT INTO postcode (postcode_id, postcode) VALUES (?, ?)",
                           (row['Postcode_ID'], row['Postcode']))

        cursor.execute("""
                CREATE TABLE talent_team (
                    talent_team_id INT,
                    forename VARCHAR(255),
                    lastname VARCHAR(255)
                );
                """)

        for index, row in talent_team.iterrows():
            cursor.execute("INSERT INTO talent_team (talent_team_id, forename, lastname) VALUES (?, ?, ?)",
                           (row['Talent_Team_ID'], row['Talent_Forename'], row['Talent_Lastname']))

        cursor.execute("""
                CREATE TABLE gender (
                    gender_id INT,
                    gender VARCHAR(255)
                );
                """)

        for index, row in gender.iterrows():
            cursor.execute("INSERT INTO gender (gender_id, gender) VALUES (?, ?)",
                           (row['Gender_ID'], row['Gender']))

        create_table_query = """
                    CREATE TABLE courses (
                        course_id INT,
                        course VARCHAR(MAX),
                        trainer_id INT
                    );
                """

        # Execute the SQL command to create the table
        cursor.execute(create_table_query)

        # Commit changes
        conn.commit()

        # Insert Dataframe into SQL Server:
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO courses (course_id, course, trainer_id) VALUES (?, ?, ?)",
                           row['Course_ID'], row['Course'], row['Trainer_ID'])


        create_table_query = """
                    DROP TABLE IF EXISTS student_weaknesses;

                    CREATE TABLE student_weaknesses (
                        student_id INT,
                        weakness_id INT
                    );
                """

        # Execute the SQL command to create the table
        cursor.execute(create_table_query)

        # Insert DataFrame data into the created SQL table (student_weaknesses)
        for index, row in df_weaknesses.iterrows():
            cursor.execute("""
                        INSERT INTO student_weaknesses (
                            student_id, weakness_id
                        ) VALUES (?, ?)
                    """,
                           row['Student_ID'], row['Weakness_ID'])

        cursor.execute(
            """
             DROP TABLE IF EXISTS student_strengths;

             CREATE Table student_strengths (
                 student_id INT,
                 strength_id INT
             );

             """
        )

        # Insert Dataframe into SQL Server:
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO student_strengths (student_id, strength_id) values(?,?)",
                           row['Student_ID'], row['Strength_ID'])

        cursor.execute(
            """
            DROP TABLE IF EXISTS Education;

            CREATE Table Education (
                student_id INT,
                university_id INT,
                grade_id INT
            );
            """
        )
        # Insert Dataframe into SQL Server:
        for index, row in Education.iterrows():
            cursor.execute("INSERT INTO Education (student_id, university_id ,grade_id) values(?,?,?)",
                           row['Student_ID'], row['University_ID'], row['Grade_ID'])

        cursor.execute("""
                CREATE TABLE address (
                    address_id INT,
                    student_id INT,
                    city_id INT,
                    postcode VARCHAR(255)
                );
                """)

        for index, row in address.iterrows():
            cursor.execute("INSERT INTO address (address_id, student_id, city_id, postcode) VALUES (?, ?, ?, ?)",
                           (row['Address_ID'], row['Student_ID'], row['City_ID'], row['Postcode']))

        cursor.execute(
            """
             DROP TABLE IF EXISTS student;

            CREATE TABLE student (
            student_id INT,
            forename VARCHAR(255),
            lastname VARCHAR(255),
            dob DATE,
            gender_id INT,
            email VARCHAR(255),
            address_id INT,
            phone_number VARCHAR(50),
            self_development BOOLEAN,
            geo_flex BOOLEAN,
            financial_support BOOLEAN,
            course_id INT,
            talent_team_id INT,
            start_date DATE
        );

             """
        )

        for index, row in student.iterrows():
            cursor.execute(
                "INSERT INTO student (student_id, forename, lastname, dob, gender_ID, email, address_id, phone_number, self_development, geo_flex, financial_support, course_id, talent_team_id, start_date) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (row['Student_ID'], row['Forename'], row['Lastname'], row['Dob'], row['Gender_ID'], row['Email'],
                 row['Address_ID'], row['Phone_Number'], row['Self_Development'], row['Geo_Flex'],
                 row['Financial_Support'], row['Course_ID'], row['Talent_Team_ID'], row['Start_Date']))

        create_table_query = """
                    DROP TABLE IF EXISTS pre_course;

                    CREATE TABLE pre_course (
                        student_id INT,
                        dob INT,
                        gender INT,
                        email VARCHAR(MAX),
                        address_id VARCHAR(MAX),
                        phone_number INT,
                        self_development BIT,
                        geo_flex BIT,
                        financial_support BIT,
                        course_id INT,
                        talent_team_id INT,
                        start_date DATE
                    );
                """

        # Execute the SQL command to create the table
        cursor.execute(create_table_query)

        # Insert DataFrame data into the created SQL table (pre_course)
        for index, row in df.iterrows():
            cursor.execute("""
                        INSERT INTO pre_course (
                            student_id, full_name, dob, gender, email, address_id,
                            phone_number, self_development, geo_flex, financial_support,
                            course_id, talent_team_id, start_date
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                           row['Student_ID'], row['Dob'], row['Gender'],
                           row['Email'], row['Address_ID'], row['Phone_Number'], row['Self_Development'],
                           row['Geo_Flex'], row['Financial_Support'], row['Course_ID'],
                           row['Talent_Team_ID'], row['Start_Date'])

        cursor.execute(
            """
             DROP TABLE IF EXISTS weekly_scores;

             CREATE Table weekly_scores (
                 weekly_scores_id INT,
                 student_id INT,
                 metric_id INT,
                 week_id INT,
                 score INT,
             );

             """
        )

        # Insert Dataframe into SQL Server:
        for index, row in df.iterrows():
            cursor.execute(
                "INSERT INTO weekly_scores (weekly_scores_id,student_id,metric_id,week_id,score) values(?,?,?,?,?)",
                row['Weekly_Scores_ID'], row['Student_ID'], row['Metric_ID'], row['Week_ID'], row['Score'])

        cursor.execute(

            """
            DROP TABLE IF EXISTS Tech_self_score;

            CREATE Table Tech_self_score (
                student_id INT,
                tech_id INT,
                score INT
            );
            """
        )

        for index, row in Tech_self_score.iterrows():
            cursor.execute("INSERT INTO Tech_self_score (student_id, tech_id, score) VALUES (?, ?, ?)",
                           row['Student_id'], row['Tech_ID'], row['Score'])




    cnxn.commit()
    cursor.close()
    cnxn.close()


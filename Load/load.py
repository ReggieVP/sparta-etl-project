import pandas as pd
import pyodbc


class Load:

    def loading_data_to_sql(self):
        server = 'localhost,1433'
        database = 'iris1'
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
                    full_name VARCHAR(255)
                );
                """)

        for index, row in trainer.iterrows():
            cursor.execute("INSERT INTO trainer (trainer_id, forename, lastname) VALUES (?, ?, ?)",
                           (row['trainer_id'], row['forename'], row['lastname']))

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
                           row['weakness_id'], row['weakness'])



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
                           row['strength_id'], row['strength'])

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
                           row['metric_id'], row['metric'])

        cursor.execute(
            """
             DROP TABLE IF EXISTS weeks;

             CREATE Table weeks (
                 week_id INTEGER,
                 week_number INTEGER 
             );

             """
        )

        # Insert Dataframe into SQL Server:
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO weeks (week_id,week_number) values(?,?)",
                           row['week_id'], row['week_number'])

        cursor.execute(
            """
            DROP TABLE IF EXISTS University;

            CREATE Table University (
                university_id INTEGER,
                university VARCHAR(MAX)
            );
            """
        )

        for index, row in University.iterrows():
            cursor.execute("INSERT INTO University (university_id, university) VALUES (?, ?)",
                           row['university_id'], row['university'])

        cursor.execute(
            """
            DROP TABLE IF EXISTS University_grade;

            CREATE Table University_grade (
                grade_id INTEGER,
                grade VARCHAR(MAX)
            );
            """
        )

        for index, row in University_grade.iterrows():
            cursor.execute("INSERT INTO University_grade (grade_id, grade) VALUES (?, ?)",
                           row['grade_id'], row['grade'])

        cursor.execute(
            """
            DROP TABLE IF EXISTS Tech;

            CREATE Table Tech (
                tech_id VARCHAR,
                tech INTEGER
            );
            """
        )

        for index, row in Tech.iterrows():
            cursor.execute("INSERT INTO Tech (tech_id, tech) VALUES (?, ?)",
                           row['tech_id'], row['tech'])

        cursor.execute("""
                CREATE TABLE city (
                    city_id INT,
                    city VARCHAR(255)
                );
                """)

        for index, row in city.iterrows():
            cursor.execute("INSERT INTO city (city_id, city) VALUES (?, ?)",
                           (row['city_id'], row['city']))

        cursor.execute("""
                CREATE TABLE postcode (
                    postcode_id INT,
                    postcode VARCHAR(255)
                );
                """)

        for index, row in postcode.iterrows():
            cursor.execute("INSERT INTO postcode (postcode_id, postcode) VALUES (?, ?)",
                           (row['postcode_id'], row['postcode']))

        cursor.execute("""
                CREATE TABLE talent_team (
                    talent_team_id INT,
                    first_name VARCHAR(255),
                    last_name VARCHAR(255)
                );
                """)

        for index, row in talent_team.iterrows():
            cursor.execute("INSERT INTO talent_team (talent_team_id, forename, last_name) VALUES (?, ?, ?)",
                           (row['talent_team_id'], row['forename'], row['lastname']))

        cursor.execute("""
                CREATE TABLE gender (
                    gender_id VARCHAR(255),
                    gender INT
                );
                """)

        for index, row in gender.iterrows():
            cursor.execute("INSERT INTO gender (gender_id, gender) VALUES (?, ?)",
                           (row['gender_id'], row['gender']))

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
                           row['course_id'], row['course'], row['trainer_id'])


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
                           row['student_id'], row['weakness_id'])

        cursor.execute(
            """
             DROP TABLE IF EXISTS student_strengths;

             CREATE Table student_strengths (
                 student_id INTEGER ,
                 strength_id INTEGER
             );

             """
        )

        # Insert Dataframe into SQL Server:
        for index, row in df.iterrows():
            cursor.execute("INSERT INTO student_strengths (student_id, strength_id) values(?,?)",
                           row['student_id'], row['strength_id'])

        cursor.execute(
            """
            DROP TABLE IF EXISTS Education;

            CREATE Table Education (
                student_id integer,
                university_id VARCHAR(MAX),
                grade_id VARCHAR(MAX)
            );
            """
        )
        # Insert Dataframe into SQL Server:
        for index, row in Education.iterrows():
            cursor.execute("INSERT INTO Education (student_id, university_id ,grade_id) values(?,?,?)",
                           row['student_id'], row['university_id'], row['grade_id'])

        cursor.execute("""
                CREATE TABLE address (
                    address_id INT,
                    student_id INT,
                    city_id VARCHAR(255),
                    postcode VARCHAR(255)
                );
                """)

        for index, row in address.iterrows():
            cursor.execute("INSERT INTO address (address_id, student_id, city_id, postcode) VALUES (?, ?, ?, ?)",
                           (row['address_id'], row['student_id'], row['city_id'], row['postcode']))

        cursor.execute(
            """
             DROP TABLE IF EXISTS student;

            CREATE TABLE student (
            student_id INT,
            forename VARCHAR(255),
            lastname VARCHAR(255),
            dob DATE,
            gender INT,
            email VARCHAR(255),
            address_id VARCHAR(255),
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
                "INSERT INTO student (student_id, forename, lastname, dob, gender, email, address_id, phone_number, self_development, geo_flex, financial_support, course_id, talent_team_id, start_date) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (row['student_id'], row['forename'], row['lastname'], row['dob'], row['gender'], row['email'],
                 row['address_id'], row['phone_number'], row['self_development'], row['geo_flex'],
                 row['financial_support'], row['course_id'], row['talent_team_id'], row['start_date']))

        create_table_query = """
                    DROP TABLE IF EXISTS pre_course;

                    CREATE TABLE pre_course (
                        student_id INT,
                        full_name VARCHAR(MAX),
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
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                           row['student_id'], row['full_name'], row['dob'], row['gender'],
                           row['email'], row['address_id'], row['phone_number'], row['self_development'],
                           row['geo_flex'], row['financial_support'], row['course_id'],
                           row['talent_team_id'], row['start_date'])

        cursor.execute(
            """
             DROP TABLE IF EXISTS weekly_scores;

             CREATE Table weekly_scores (
                 weekly_scores_id INTEGER,
                 student_id INTEGER,
                 metric_id INTEGER,
                 week_id INTEGER,
                 score INTEGER,
             );

             """
        )

        # Insert Dataframe into SQL Server:
        for index, row in df.iterrows():
            cursor.execute(
                "INSERT INTO weekly_scores (weekly_scores_id,student_id,metric_id,week_id,score) values(?,?,?,?,?)",
                row['weekly_scores_id'], row['student_id'], row['metric_id'], row['week_id'], row['score'])

        cursor.execute(

            """
            DROP TABLE IF EXISTS Tech_self_score;

            CREATE Table Tech_self_score (
                student_id INTEGER,
                tech_id VARCHAR(MAX),
                score INTEGER
            );
            """
        )

        for index, row in Tech_self_score.iterrows():
            cursor.execute("INSERT INTO Tech_self_score (student_id, tech_id, score) VALUES (?, ?, ?)",
                           row['student_id'], row['tech_id'], row['score'])




    cnxn.commit()
    cursor.close()
    cnxn.close()


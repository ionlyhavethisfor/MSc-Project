# -*- coding: utf-8 -*-
"""
Created on Sun Sep 29 00:57:36 2024.

@author: caspe
"""

# =============================================================================
# Uploaded w/ a database already made.
# If you decide to make a new one, and this doesn't work, just contact me.
# =============================================================================

import os
import json
import sqlite3
import webvtt
import gc
import dateparser


# Probably disregard; I tried halfheartedly to store the dates in some other format & do cleaning but think I gave up.
def convert_date(date):
    if date:
        parsed_date = dateparser.parse(date)
        if parsed_date:
            formatted_date = int(parsed_date.strftime("%Y%m%d"))
            return formatted_date
    else:
        return None


print(convert_date('Oct 1, 1995'))

# %% Defining functions for loading database.


def make_bio_table(dbname, folder_path, batch_size=2000):
    """Generate and populate the biographical table."""
    print("Running BioTable")
    conn = sqlite3.connect(dbname, timeout=100)
    cursor = conn.cursor()
    # Create BioTable if it doesn't exist
    cursor.execute("PRAGMA journal_mode = WAL;")
    cursor.execute("PRAGMA synchronous = NORMAL;")
    cursor.execute("PRAGMA cache_size = -50000;")  # 200MB cache
    cursor.execute("PRAGMA temp_store = MEMORY;")
    cursor.execute("PRAGMA locking_mode = EXCLUSIVE;")
    cursor.execute("PRAGMA defer_foreign_keys = ON;")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS BioTable (
        PIQPersonID INTEGER,
        FullName TEXT,
        Gender TEXT,
        CityOfBirth TEXT,
        CountryOfBirth TEXT,
        DateOfBirth TEXT,
        DOBINT INTEGER,
        ExperienceGroup TEXT,
        ImageURL TEXT,
        LanguageLabel TEXT,
        IntCode INTEGER,
        InterviewDate TEXT,
        Aliases TEXT,
        InterviewLength INTEGER,
        InVHAOnline TEXT,
        Interviewers TEXT,
        InterviewLocation TEXT,
        OrganizationName TEXT
    )
    ''')

    count = 0
    batch_data = []  # List to hold batch data
    cursor.execute("BEGIN TRANSACTION;")
    for filename in os.listdir(folder_path):
        if count % 2000 == 0:
            print(f"{count} files processed so far")

        if filename.endswith(".json"):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r') as f:
                try:
                    data = json.load(f)
                    testimony_data = data["Testimony"]
                    bio = data["Bio"]
                    # Prepare a row of data
                    bio_row = (
                        bio["PIQPersonID"],
                        bio.get("FullName"),
                        bio.get("Gender"),
                        bio.get("CityOfBirth"),
                        bio.get("CountryOfBirth"),
                        bio.get('DateOfBirthText'),
                        convert_date(bio.get('DateOfBirthText')),
                        bio.get("ExperienceGroup"),
                        bio.get("ImageURL"),
                        testimony_data.get("LanguageLabel"),
                        testimony_data.get("IntCode"),
                        str(testimony_data.get("InterviewDate")),
                        str(bio.get('Aliases')),
                        testimony_data.get('InterviewLength'),
                        str(testimony_data.get('InVHAOnline')),
                        str(testimony_data.get('Interviewers')),
                        str(testimony_data.get('InterviewLocation')),
                        str(testimony_data.get('OrganizationName')),
                    )

                    batch_data.append(bio_row)  # Add row to batch

                    # If the batch reaches the batch_size, execute batch insert
                    if len(batch_data) >= batch_size:
                        cursor.executemany('''
                        INSERT OR REPLACE INTO BioTable (PIQPersonID, FullName, Gender,
                                                         CityOfBirth, CountryOfBirth,
                                                         DateOfBirth, DOBINT, ExperienceGroup,
                                                         ImageURL, LanguageLabel, IntCode,
                                                         InterviewDate, Aliases,
                                                         InterviewLength, InVHAOnline,
                                                         Interviewers, InterviewLocation,
                                                         OrganizationName)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', batch_data)
                        conn.commit()
                        batch_data = []

                    count += 1

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from file {filename}: {e}")
                except sqlite3.Error as e:
                    print(f"SQLite error: {e}")
                    conn.rollback()
    # Final commit for any remaining data
    if batch_data:
        cursor.executemany('''
        INSERT OR REPLACE INTO BioTable (PIQPersonID, FullName, Gender,
                                         CityOfBirth, CountryOfBirth,
                                         DateOfBirth, DOBINT, ExperienceGroup,
                                         ImageURL, LanguageLabel, IntCode,
                                         InterviewDate, Aliases,
                                         InterviewLength, InVHAOnline,
                                         Interviewers, InterviewLocation,
                                         OrganizationName)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', batch_data)
        conn.commit()
    cursor.execute("PRAGMA optimize;")
    cursor.close()
    conn.close()


def make_question_table(dbname, folder_path):
    """Generate and populate the questions table."""
    print("Running QuestionsTable")
    conn = sqlite3.connect(dbname, timeout=10)
    cursor = conn.cursor()
    cursor.execute("PRAGMA journal_mode = WAL;")
    cursor.execute("PRAGMA synchronous = NORMAL;")
    cursor.execute("PRAGMA cache_size = -50000;")
    cursor.execute("PRAGMA temp_store = MEMORY;")
    cursor.execute("PRAGMA locking_mode = EXCLUSIVE;")
    cursor.execute("PRAGMA defer_foreign_keys = ON;")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS QuestionsTable (
        PIQPersonID INTEGER,
        IntCode INTEGER,
        QuestionText TEXT,
        Answer TEXT,
        PRIMARY KEY (PIQPersonID, IntCode, QuestionText, Answer)
    )
    ''')
    log_list = []
    count = 0
    cursor.execute("BEGIN TRANSACTION;")
    for filename in os.listdir(folder_path):
        if count % 2000 == 0:
            print(f"{count} files so far")
        count += 1
        if filename.endswith(".json"):  # Only process JSON files
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r') as f:
                try:
                    # Load the JSON data
                    data = json.load(f)

                    # Extract the question data
                    questiondata = data["Bio"].get("BioQuestionAnswers", None)
                    if questiondata is None:
                        print(f"No QuestionsTable found in file: {filename}")
                        continue
                    elif not isinstance(questiondata, list):
                        print(f"QuestionsTable is not a list in file: {filename}")
                        continue

                    for qa in questiondata:
                        question_text = qa.get("QuestionText")
                        answer = qa.get("Answers")

                        # Handle case where answers is a list
                        if isinstance(answer, list):
                            for answ in answer:
                                try:
                                    cursor.execute('''
                                    INSERT INTO QuestionsTable (PIQPersonID, Intcode, QuestionText, Answer)
                                    VALUES (?, ?, ?, ?)
                                    ''', (
                                        data["Bio"]["PIQPersonID"],
                                        data["Testimony"]["IntCode"],
                                        question_text,
                                        answ
                                    ))
                                except sqlite3.IntegrityError:
                                    log_list.append((
                                        data["Bio"]["PIQPersonID"],
                                        data["Testimony"]["IntCode"],
                                        question_text,
                                        answ))
                                    continue

                        else:
                            # Handle case where answer is not a list (or None)
                            if answer is None:
                                # answer = 'None/Unanswered'
                                continue
                            elif isinstance(answer, dict):
                                # print(question_text, str(answer))
                                continue  # all the results are @xsi:nil which indicates null values afaik

                            cursor.execute('''
                            INSERT INTO QuestionsTable (PIQPersonID, IntCode, QuestionText, Answer)
                            VALUES (?, ?, ?, ?)
                            ''', (
                                data["Bio"]["PIQPersonID"],
                                data["Testimony"]["IntCode"],
                                question_text,
                                answer
                            ))
                    conn.commit()

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from file {filename}: {e}")
                except sqlite3.Error as e:
                    print(f"SQLite error: {e}")
                    conn.rollback()
    cursor.execute("PRAGMA optimize;")
    cursor.close()
    conn.close()
    print(f"{len(log_list)} constraint failures found.")
    return log_list


def make_people_table(dbname, folder_path):
    """Generate and populate the table of interviewee known people."""
    print("Running PeopleTable")
    # Connect to the SQLite database
    conn = sqlite3.connect(dbname, timeout=10)
    cursor = conn.cursor()
    # Create the KeywordsTable if it doesn't already exist
    cursor.execute("PRAGMA journal_mode = WAL;")
    cursor.execute("PRAGMA synchronous = NORMAL;")
    cursor.execute("PRAGMA cache_size = -50000;")  # 200MB cache
    cursor.execute("PRAGMA temp_store = MEMORY;")
    cursor.execute("PRAGMA locking_mode = EXCLUSIVE;")
    cursor.execute("PRAGMA defer_foreign_keys = ON;")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS PeopleTable (
        PIQPersonID INTEGER,
        IntCode INTEGER,
        TapeNumber INTEGER,
        SegmentNumber INTEGER,
        RelationName TEXT,
        Relationship TEXT,
        RelationPIQ INTEGER,
        PRIMARY KEY (PIQPersonID, RelationPIQ)
    )
    ''')
    log_list = []
    count = 0

    cursor.execute("BEGIN TRANSACTION;")
    for filename in os.listdir(folder_path):
        if count % 2000 == 0:
            print(f"{count} files processed so far")
        count += 1

        if filename.endswith(".json"):  # Process only JSON files
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r') as f:
                try:
                    # Load the JSON data
                    data = json.load(f)
                    piq = data["Bio"]["PIQPersonID"]
                    interview_code = data["Testimony"]["IntCode"]

                    for segment in data["Segments"]:
                        tape_num = segment["TapeNumber"]
                        segment_num = segment["SegmentNumber"]
                        if segment.get("Indexes") is not None:
                            indexdata = segment["Indexes"]

                            # Process Keywords if they exist
                            if indexdata.get("People") is not None:
                                for person in indexdata["People"]:
                                    try:
                                        relation_piq = person["PIQPersonID"]
                                        relation_name = person["FullName"]
                                        relationship = person.get("Relationship", None)

                                        cursor.execute('''
                                            INSERT INTO PeopleTable
                                                (PIQPersonID, IntCode,
                                                TapeNumber, SegmentNumber,
                                                RelationName, RelationShip,
                                                RelationPIQ)
                                            VALUES (?, ?, ?, ?, ?, ?, ?)
                                            ''', (
                                                piq,
                                                interview_code,
                                                tape_num,
                                                segment_num,
                                                relation_name,
                                                relationship,
                                                relation_piq
                                            ))
                                    except sqlite3.IntegrityError:
                                        log_list.append((piq, relation_piq))
                            else:
                                continue
                        else:

                            continue
                        # Insert the keyword into the KeywordsTable
                    conn.commit()

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from file {filename}: {e}")
                except sqlite3.Error as e:
                    print(f"SQLite error: {e}")
                    conn.rollback()  # Roll back any partial transaction on SQLite error
                except Exception as e:
                    print(f"Unexpected error: {e}")
    # Close the database connection
    print(f"{len(log_list)} constraint failures found.")
    cursor.execute("PRAGMA optimize;")
    cursor.close()
    conn.close()


def make_keywords_table(dbname, folder_path, batch_size=2000):
    """Generate and populate table of keywords."""
    print("Running KeywordsTable")
    conn = sqlite3.connect(dbname, timeout=10)
    cursor = conn.cursor()
    # Create the KeywordsTable if it doesn't already exist
    cursor.execute("PRAGMA journal_mode = WAL;")
    cursor.execute("PRAGMA synchronous = NORMAL;")
    cursor.execute("PRAGMA cache_size = -50000;")
    cursor.execute("PRAGMA temp_store = MEMORY;")
    cursor.execute("PRAGMA locking_mode = EXCLUSIVE;")
    cursor.execute("PRAGMA defer_foreign_keys = ON;")
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS KeywordsTable (
        PIQPersonID INTEGER,
        IntCode INTEGER,
        TapeNumber INTEGER,
        SegmentNumber INTEGER,
        SegmentID INTEGER,
        KeywordID INTEGER,
        KeywordLabel TEXT,
        Latitude REAL,
        Longitude REAL,
        PRIMARY KEY (KeywordID, PIQPersonID)
    )
    ''')

    count = 0
    batch_data = []  # A list to hold rows before batch inserting

    cursor.execute("BEGIN TRANSACTION;")
    for filename in os.listdir(folder_path):
        if count % 2000 == 0:
            print(f"{count} files processed so far")
        count += 1

        if filename.endswith(".json"):  # Process only JSON files
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r') as f:
                try:
                    # Load the JSON data
                    data = json.load(f)
                    piq = data["Bio"]["PIQPersonID"]
                    interview_code = data["Testimony"]["IntCode"]

                    for segment in data["Segments"]:
                        tape_num = segment["TapeNumber"]
                        segment_num = segment["SegmentNumber"]
                        segment_id = segment["SegmentID"]
                        if segment.get("Indexes") is not None:
                            indexdata = segment["Indexes"]

                            # Process Keywords if they exist
                            if indexdata.get("Keywords") is not None:
                                keywords = indexdata["Keywords"]
                                for keyword in keywords:
                                    keyword_id = keyword["KeywordID"]
                                    keyword_label = keyword["KeywordLabel"]
                                    latitude = None
                                    longitude = None
                                    if keyword.get("Coordinate") is not None:
                                        latitude = keyword["Coordinate"]["Latitude"]
                                        longitude = keyword["Coordinate"]["Longitude"]

                                    # Append the row to batch_data
                                    batch_data.append((
                                        piq,
                                        interview_code,
                                        tape_num,
                                        segment_num,
                                        segment_id,
                                        keyword_id,
                                        keyword_label,
                                        latitude,
                                        longitude
                                    ))

                                    # If batch_data reaches the batch_size, execute batch insert
                                    if len(batch_data) >= batch_size:
                                        cursor.executemany('''
                                            INSERT OR IGNORE INTO KeywordsTable (PIQPersonID, IntCode,
                                                                       TapeNumber, SegmentNumber,
                                                                       SegmentID, KeywordID,
                                                                       KeywordLabel, Latitude, Longitude)
                                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                                        ''', batch_data)
                                        conn.commit()  # Commit the batch
                                        batch_data.clear()  # Clear the batch after committing
                            else:
                                continue
                        else:
                            continue

                except json.JSONDecodeError as e:
                    print(f"Error decoding JSON from file {filename}: {e}")
                except sqlite3.Error as e:
                    print(f"SQLite error: {e}")
                    conn.rollback()

    # Insert any remaining rows that didn't make it into the last batch
    if batch_data:
        cursor.executemany('''
            INSERT OR IGNORE INTO KeywordsTable (PIQPersonID, IntCode, TapeNumber,
                                       SegmentNumber, SegmentID, KeywordID,
                                       KeywordLabel, Latitude, Longitude)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', batch_data)
        conn.commit()
    # Close the database connection
    cursor.execute("PRAGMA optimize;")
    cursor.close()
    conn.close()


def make_testimony_table(dbname, directory_path, batch_size=2000):
    """Generate and populate table of testimonies."""
    # Connect to the SQLite database
    conn = sqlite3.connect(dbname, timeout=10)
    cursor = conn.cursor()
    count = 0
    # Create the TestimonyTable table if it does not exist
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS TestimonyTable (
        IntCode INTEGER,
        TapeNumber INTEGER,
        TapeTestimony TEXT,
        PRIMARY KEY (IntCode, TapeNumber)
    )
    ''')

    batch_data = []
    cursor.execute("BEGIN TRANSACTION;")
    for filename in os.listdir(directory_path):
        if count % 2000 == 0:
            print(f"{count} files processed so far")
        count += 1
        file_path = os.path.join(directory_path, filename)
        split_path = filename.split('-')
        interview_code = int(split_path[0])
        tape_number = int(split_path[1].split('.')[0])

        if count % 500 == 0:
            print(interview_code, tape_number)

        text = ""

        for caption in webvtt.read(file_path):
            text += " " + caption.text
        text = text.replace("&#39;", "'")

        batch_data.append((interview_code, tape_number, text))

        if len(batch_data) >= batch_size:
            cursor.executemany('''
            INSERT INTO TestimonyTable (IntCode, TapeNumber, TapeTestimony)
            VALUES (?, ?, ?)
            ''', batch_data)
            conn.commit()  # Commit the batch
            batch_data = []  # Reset the batch data

    if batch_data:
        cursor.executemany('''
        INSERT INTO TestimonyTable (IntCode, TapeNumber, TapeTestimony)
        VALUES (?, ?, ?)
        ''', batch_data)
        conn.commit()
    cursor.execute("PRAGMA optimize;")
    cursor.close()
    conn.close()

# %% Write database file.


path_root = "VHA/VHA/metadata/"
db_name = "databases/test6.db"

make_bio_table(db_name, path_root + "en")
make_bio_table(db_name, path_root + "de")
make_bio_table(db_name, path_root + "cs")
make_bio_table(db_name, path_root + "nl")

question_log = make_question_table(db_name, path_root + "en")
make_question_table(db_name, path_root + "de")
make_question_table(db_name, path_root + "cs")
make_question_table(db_name, path_root + "nl")

keywords_log = make_keywords_table(db_name, path_root + "en")
make_keywords_table(db_name, path_root + "de")
make_keywords_table(db_name, path_root + "cs")
make_keywords_table(db_name, path_root + "nl")

people_log = make_people_table(db_name, path_root + "en")
make_people_table(db_name, path_root + "de")
make_people_table(db_name, path_root + "cs")
make_people_table(db_name, path_root + "nl")

make_testimony_table(db_name, "VHA/VHA/English.batch1/English/batch1/")
make_testimony_table(db_name, "VHA/VHA/English.batch2/English/batch2/")
make_testimony_table(db_name, "VHA/VHA/English.batch3/English/batch3/")
make_testimony_table(db_name, "VHA/VHA/English.batch4/English/batch4/")
make_testimony_table(db_name, "VHA/VHA/English.batch5/English/batch5/")
make_testimony_table(db_name, "VHA/VHA/English.batch6/English/batch6/")
make_testimony_table(db_name, "VHA/VHA/German/German/")
make_testimony_table(db_name, "VHA/VHA/Czech/Czech")



#%%
def create_index(db_name, table, column, indexname):
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    create_index_sql = f"""
    CREATE INDEX IF NOT EXISTS {indexname} ON {table}({column})
    """
    cursor.execute(create_index_sql)
    conn.commit()
    cursor.execute("PRAGMA optimize;")
    conn.close()
    return

# %% Creating Indexes
# TestimonyTable Indexes


create_index(db_name, 'TestimonyTable', 'IntCode', 'idx_intcode')
create_index(db_name, 'TestimonyTable', 'PIQPersonID', 'idx_piq')


# %%
# QUESTIONSTABLE INDEXES

create_index(db_name, 'QuestionsTable', 'QuestionText, Answer', 'idx_qa')
create_index(db_name, 'QuestionsTable', 'PIQPersonID', 'idx_piq')

# %%
# BIOTABLE INDEXES

create_index(db_name, 'BioTable', 'PIQPersonID', 'idx_piq')
create_index(db_name, 'BioTable', 'DateOfBirth', 'idx_birthdate')
create_index(db_name, 'BioTable', 'LanguageLabel', 'idx_lang')
create_index(db_name, 'BioTable', 'ExperienceGroup', 'idx_exp')
create_index(db_name, 'BioTable', 'CountryOfBirth', 'idx_country')
create_index(db_name, 'BioTable', 'CityOfBirth', 'idx_city')
create_index(db_name, 'BioTable', 'FullName', 'idx_name')

# %%
# KEYWORDSTABLE INDEXES

create_index(db_name, 'KeywordsTable', 'IntCode', 'idx_intcode')
create_index(db_name, 'KeywordsTable', 'PIQPersonID', 'idx_piq')
create_index(db_name, 'KeywordsTable', 'KeywordID', 'idx_kwid')
create_index(db_name, 'KeywordsTable', 'KeywordLabel', 'idx_kwlab')
create_index(db_name, 'KeywordsTable', 'RootID', 'idx_rootid')

create_index(db_name, 'KeywordsTable', 'KeywordLabel, PIQPersonID, Latitude', 'idx_lab_piq_lat')
# %% TESTING IF INDEXING SUCCESSFUL

# conn = sqlite3.connect(db_name)
# cursor = conn.cursor()
# cursor.execute("PRAGMA index_list('KeywordsTable');")
# indexes = cursor.fetchall()
# for index in indexes:
#     print(index)
# conn.close()

# %% Dropping a table

# connection = sqlite3.connect(db_name)
# cursor = connection.cursor()
# table_name = ''
# cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
# cursor.execute('VACUUM;')
# connection.commit()
# cursor.close()
# connection.close()
# gc.collect()
# print(f"Table '{table_name}' has been dropped (if it existed).")

# %% Adding PIQPersonID to testimonytable and making fts5 table for in-text search.
conn = sqlite3.connect(db_name)
cursor = conn.cursor()
cursor.execute("PRAGMA journal_mode = WAL;")
cursor.execute("PRAGMA synchronous = NORMAL;")
cursor.execute("PRAGMA cache_size = -50000;")  # 200MB cache
cursor.execute("PRAGMA temp_store = MEMORY;")
cursor.execute("PRAGMA locking_mode = EXCLUSIVE;")
cursor.execute("PRAGMA defer_foreign_keys = ON;")
cursor.execute("DROP TABLE IF EXISTS TestimonyTable_fts;")
cursor.execute('ALTER TABLE TestimonyTable ADD COLUMN PIQPersonID INTEGER;')
cursor.execute('''
    UPDATE TestimonyTable
    SET PIQPersonID = (
        SELECT PIQPersonID
        FROM BioTable
        WHERE BioTable.IntCode = TestimonyTable.IntCode
    )
    WHERE IntCode IS NOT NULL;
''')

cursor.execute('''
    CREATE VIRTUAL TABLE IF NOT EXISTS TestimonyTable_fts
    USING fts5(
        TapeTestimony,
        PIQPersonID,
        IntCode UNINDEXED,
        tokenize="unicode61 remove_diacritics 1"
    );
''')

# Clear existing data in the FTS table
cursor.execute('DELETE FROM TestimonyTable_fts')

# Insert data into the FTS table, grouping testimony by each person
cursor.execute('''
    INSERT INTO TestimonyTable_fts (TapeTestimony, PIQPersonID, IntCode)
    SELECT
        GROUP_CONCAT(TapeTestimony, ' ') AS TapeTestimony,  -- Concatenate all testimony texts for each person
        PIQPersonID,
        IntCode
    FROM TestimonyTable
    GROUP BY PIQPersonID, IntCode  -- Group by the unique person identifier (PIQPersonID or IntCode)
''')

cursor.execute("INSERT INTO TestimonyTable_fts(TestimonyTable_fts) VALUES('optimize');")
conn.commit()
cursor.execute("PRAGMA optimize;")
conn.close()

# %% VACUUMING, probably takes a while. I dunno.

# conn = sqlite3.connect(db_name)
# cursor = conn.cursor()
# cursor.execute("VACUUM;")
# conn.commit()
# cursor.execute("PRAGMA optimize;")
# conn.close()


# %% Keyword hierarchy & stuff.
with open('vha_thesaurus/kwhierarchy_vha.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

lowest_terms = []
for i in data:
    lowest_terms.append(i['CategoryRootKeywordLabel'])

def flatten_to_list_of_dicts(nested_list):
    flat_list = []

    def flatten_helper(d):
        if isinstance(d, list):
            for item in d:
                flatten_helper(item)
        elif isinstance(d, dict):
            flat_list.append(d)
            for value in d.values():
                flatten_helper(value)

    flatten_helper(nested_list)
    return flat_list


flattened_data = flatten_to_list_of_dicts(data)


conn = sqlite3.connect(db_name)
cursor = conn.cursor()
cursor.execute("PRAGMA journal_mode = WAL;")
cursor.execute("PRAGMA synchronous = NORMAL;")
cursor.execute("PRAGMA page_size = 8192;")
cursor.execute("PRAGMA cache_size = 10000;")
cursor.execute("""
ALTER TABLE KeywordsTable
ADD COLUMN RootID INTEGER;
""")
cursor.execute("""
ALTER TABLE KeywordsTable
ADD COLUMN RootLabel TEXT;
""")
cursor.execute("""
ALTER TABLE KeywordsTable
ADD COLUMN ParentID INTEGER;
""")
cursor.execute("""
ALTER TABLE KeywordsTable
ADD COLUMN ParentLabel TEXT;
""")
for item in flattened_data:
    rootlab = item['CategoryRootKeywordLabel']
    rootid = item['CategoryRootKeywordID']
    parlab = item['ParentLabel']
    parid = item['ParentID']
    assoc_id = item['KeywordID']
    # Update the table by setting the 'value' column where the 'name' column matches
    query = """
    UPDATE KeywordsTable
    SET RootLabel = ?, RootID = ?, ParentLabel = ?, ParentID = ?
    WHERE KeywordID = ?
    """
    cursor.execute(query, (rootlab, rootid, parlab, parid, assoc_id))

conn.commit()
cursor.execute("PRAGMA optimize;")
cursor.close()
conn.close()


conn = sqlite3.connect(db_name)
cursor = conn.cursor()
cursor.execute("""
               CREATE TABLE root_keywords AS
               SELECT DISTINCT KeywordLabel, KeywordID, RootID
               FROM KeywordsTable;
               """)
cursor.execute("CREATE INDEX IF NOT EXISTS idx_root ON root_keywords(RootID)")
cursor.execute("PRAGMA optimize;")
cursor.close()
conn.close()


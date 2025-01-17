import sqlite3
conn=sqlite3.connect('test.sqlite3')
cursor=conn.cursor()

cursor.execute("CREATE TABLE IF NOT EXISTS users (id INTEGER PRIMARY KEY, name TEXT, age INTEGER);")
cursor.executemany("INSERT INTO users (name, age) VALUES (?, ?);", 
                   [('Jake', 30), ('Bob', 25), ('Wendy', 35), ('Alice', 28), ('Tom', 32), 
                    ('Emma', 24), ('Chris', 29), ('Sophia', 31), ('David', 33), ('Zoe', 27),
                    ('Jake', 30), ('Bob', 25), ('Wendy', 35), ('Alice', 28), ('Tom', 32), 
                    ('Emma', 24), ('Chris', 29), ('Sophia', 31), ('David', 33), ('Zoe', 27),
                    ('Jake', 30), ('Bob', 25), ('Wendy', 35), ('Alice', 28), ('Tom', 32), 
                    ('Emma', 24), ('Chris', 29), ('Sophia', 31), ('David', 33), ('Zoe', 27),
                    ('Jake', 30), ('Bob', 25), ('Wendy', 35), ('Alice', 28), ('Tom', 32), 
                    ('Emma', 24), ('Chris', 29), ('Sophia', 31), ('David', 33), ('Zoe', 27),
                    ('Jake', 30), ('Bob', 25), ('Wendy', 35), ('Alice', 28), ('Tom', 32), 
                    ('Emma', 24), ('Chris', 29), ('Sophia', 31), ('David', 33), ('Zoe', 27),
                    ('Jake', 30), ('Bob', 25), ('Wendy', 35), ('Alice', 28), ('Tom', 32), 
                    ('Emma', 24), ('Chris', 29), ('Sophia', 31), ('David', 33), ('Zoe', 27),
                    ('Jake', 30), ('Bob', 25), ('Wendy', 35), ('Alice', 28), ('Tom', 32), 
                    ('Emma', 24), ('Chris', 29), ('Sophia', 31), ('David', 33), ('Zoe', 27),
                    ('Jake', 30), ('Bob', 25), ('Wendy', 35), ('Alice', 28), ('Tom', 32), 
                    ('Emma', 24), ('Chris', 29), ('Sophia', 31), ('David', 33), ('Zoe', 27),
                    ('Jake', 30), ('Bob', 25), ('Wendy', 35), ('Alice', 28), ('Tom', 32), 
                    ('Emma', 24), ('Chris', 29), ('Sophia', 31), ('David', 33), ('Zoe', 27),
                    ('Jake', 30), ('Bob', 25), ('Wendy', 35), ('Alice', 28), ('Tom', 32), 
                    ('Emma', 24), ('Chris', 29), ('Sophia', 31), ('David', 33), ('Zoe', 27),
                    ('Jake', 30), ('Bob', 25), ('Wendy', 35), ('Alice', 28), ('Tom', 32), 
                    ('Emma', 24), ('Chris', 29), ('Sophia', 31), ('David', 33), ('Zoe', 27),
                    ('Jake', 30), ('Bob', 25), ('Wendy', 35), ('Alice', 28), ('Tom', 32), 
                    ('Emma', 24), ('Chris', 29), ('Sophia', 31), ('David', 33), ('Zoe', 27),
                    ('Jake', 30), ('Bob', 25), ('Wendy', 35), ('Alice', 28), ('Tom', 32), 
                    ('Emma', 24), ('Chris', 29), ('Sophia', 31), ('David', 33), ('Zoe', 27),
                    ('Jake', 30), ('Bob', 25), ('Wendy', 35), ('Alice', 28), ('Tom', 32),])

conn.commit()
conn.close()


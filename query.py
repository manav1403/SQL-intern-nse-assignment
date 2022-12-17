# connect to sql database

import sqlite3
conn = sqlite3.connect('nse.db')
c = conn.cursor()

#Question 1:
c.execute("SELECT ISIN , ((CLOSE - OPEN) / OPEN) AS GAIN FROM bavcopy WHERE TIMESTAMP = (SELECT MAX(TIMESTAMP) FROM bavcopy) ORDER BY ((CLOSE - OPEN) / OPEN) DESC LIMIT 25")
print("Question 1: Answer")
rows = c.fetchall()
with open('Question1_result.csv', 'w') as f:
    f.write('ISIN,GAIN\n')
    for row in rows:
        f.write(",".join(map(str,row)) + '\n')

# Question 2:
c.execute("SELECT ISIN , ((CLOSE - OPEN) / OPEN) AS GAIN FROM bavcopy ORDER BY ((CLOSE - OPEN) / OPEN) DESC LIMIT 25")
print("Question 2: Answer")
rows = c.fetchall()
with open('Question2_result.csv', 'w') as f:
    f.write('ISIN,GAIN\n')
    for row in rows:
        f.write(",".join(map(str,row)) + '\n')

#Question 3:
c.execute("SELECT A.ISIN, ((A.CLOSE - B.OPEN) / A.OPEN) AS GAIN FROM (SELECT * FROM bavcopy WHERE TIMESTAMP = (SELECT MAX(TIMESTAMP) FROM bavcopy) ) as A JOIN \
(SELECT * FROM bavcopy WHERE TIMESTAMP = (SELECT MIN(TIMESTAMP) FROM bavcopy) ) as B ON A.ISIN = B.ISIN \
ORDER BY ((A.CLOSE - B.OPEN) / A.OPEN) DESC LIMIT 25")
print("Question 3: Answer")
rows = c.fetchall()

#write the result to csv file
with open('Question3_result.csv', 'w') as f:
    f.write('ISIN,GAIN\n')
    for row in rows:
        f.write(",".join(map(str,row)) + '\n')

# close connection
conn.close()
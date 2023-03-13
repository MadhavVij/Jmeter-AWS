import os

import hashlib
import time
import memcache
import pymysql
from flask import Flask, render_template, request

app = Flask(__name__)
memC = memcache.Client([''], debug=0)


def connectDB():
    return pymysql.connect(host='', port=3306, user='',
                           password='', db='', local_infile=True)

def createDB():
    conn = connectDB()
    cur = conn.cursor()
    cur.execute("""DROP TABLE IF EXISTS data""")
    conn.commit()
    query = """ CREATE TABLE Classes (
    `Branch` VARCHAR(4) CHARACTER SET utf8,
    `Course` INT,
    `Section` INT,
    `Course_Title` VARCHAR(31) CHARACTER SET utf8,
    `Instructor` VARCHAR(12) CHARACTER SET utf8,
    `Day_s` VARCHAR(4) CHARACTER SET utf8,
    `Start_time` VARCHAR(8) CHARACTER SET utf8,
    `End_Time` VARCHAR(8) CHARACTER SET utf8,
    `Max` INT,
    `Enrolled` INT
      ); """

    cur.execute(query)
    conn.commit()
    query = """ LOAD DATA LOCAL INFILE '/home/ubuntu/quiz7/input/Classes.csv' INTO TABLE
                  Classes FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '"' ESCAPED
                  BY '"' Lines terminated by '\n' IGNORE 1 LINES; """
    cur.execute(query)
    conn.commit()
    cur.close()
    conn.close()


def fromDB(sql):
    conn = connectDB()
    cur = conn.cursor()
    cur.execute(sql)
    data = cur.fetchall()
    conn.commit()
    cur.close()
    conn.close()
    return data


def fromMemcache(sql):
    conn = connectDB()
    cur = conn.cursor()

    hash = hashlib.sha256(sql).hexdigest()
    # print(hash)
    key = f'cache:{hash}'
    if not memC.get(key):
        # print("add to memcache")
        cur.execute(sql)
        data = cur.fetchall()
        conn.commit()
        cur.close()
        conn.close()
        memC.set(key, data, time=500)

    # print("used memcache")
    return memC.get(key)


@app.route('/getAll', methods=['POST','GET'])
def exampleDB():
    start_time = time.time()
    result = 0
    output = []
    if request.method == 'POST':
        instructor = request.form['name'] or 'xxxxxx'
        course = request.form['course'] or '0000'
        query = f'select * from Classes WHERE Instructor like "%{instructor}%" or Course = "{course}"'
        result = fromMemcache(query)
        for row in result:
            tuple = (row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9])
            output.append(tuple)
    end_time = time.time()
    total_time = end_time-start_time
    print(total_time)
    output.append(total_time)
    return render_template('display.html', output=output)




@app.route('/')
def main():
    #createDB()
    return render_template('index.html')


port = os.getenv('PORT', '80')
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(port))

#####################################################################


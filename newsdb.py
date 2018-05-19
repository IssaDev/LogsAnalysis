#!/usr/bin/env python3
import psycopg2
import math
DBNAME = "news"


def popular_articles():
    # make call to the database to retrieve the most popular articles
    sql_statement = "select articles.title, count(*) as views from articles join log on concat('/article/', articles.slug) = log.path group by articles.title order by views DESC"  # noqa
    result = connect_db(sql_statement)
    with open("news_output.txt", "a") as text_file:
        text_file.write("The most poular articles of all time are:" + '\r\n')
        for x in range(0, 3):
            title = str(result[x][0])
            views = str(result[x][1])
            text_file.write(title + " - " + views + " views" + '\r\n')


def popular_authors():
    # make call to the database to get the most popular article authors
    sql_statement = "select authors.name, count(*) as views from articles, authors, log where concat('/article/',articles.slug) = log.path and authors.id = articles.author group by authors.name order by views DESC"  # noqa
    result = connect_db(sql_statement)
    with open("news_output.txt", "a") as text_file:
        text_file.write("============================================================" + '\r\n')  # noqa
        text_file.write("The most popular article authors are: " + '\r\n')
        for y in range(0, len(result)):
            name = str(result[y][0])
            views = str(result[y][1])
            text_file.write(name + " - " + views + " views" + '\r\n')


def error_days():
    # make call to the database to retrieve days where request lead to errors, and total requests made that day # noqa
    sql_statement = "WITH table1 AS (select DATE(time), count(*) as total_requests from log group by DATE(time) order by total_requests DESC),table2 AS (select status, DATE(time), count(*) as requests from log group by status,DATE(time) having status = '404 NOT FOUND' order by requests DESC)select total_requests,requests,to_char(table1.date::DATE, 'Mon dd, yyyy') from table1 inner join table2 on table1.date = table2.date"  # noqa
    rez = connect_db(sql_statement)
    with open("news_output.txt", "a") as text_file:
        text_file.write("============================================================" + '\r\n')  # noqa
        text_file.write("Days with more than 1% erros are: " + '\r\n')
        # calcualte error request percentage for the day
        for e in range(0, len(rez)):
            pcent = float(rez[e][1]) / float(rez[e][0])
            pcent = pcent * 100
            pcent = round(pcent)
            if(pcent > 1.0):
                error_percentage = "{} - {}% errors".format(rez[e][2], pcent)
                text_file.write(error_percentage + '\r\n')


def connect_db(sql_statement):
    try:
        db = psycopg2.connect(database=DBNAME)
        curz = db.cursor()
        curz.execute(sql_statement)  # noqa
        rez = curz.fetchall()
        db.close()
        return rez
    except:
        print("Failed to connect to the database")

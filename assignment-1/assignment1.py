import psycopg2
from datetime import datetime

def load_csv_into_postgres(path):
    db_conn_obj=psycopg2.connect(dbname="myfiles",
                                    host="localhost",
                                    port="5432",
                                    )
    with db_conn_obj.cursor() as curs:
        query="""CREATE TABLE if not exists hourly_rainfall_data( date DATE,
                                                                    hour INTEGER,
                                                                    rain_mm FLOAT);
                                                            COPY hourly_rainfall_data  
                                                                    FROM '{path_to_csv}' 
                                                                            DELIMITER ','
                                                                            CSV HEADER;""".format(path_to_csv=path)
        curs.execute(query)
    db_conn_obj.commit()
    db_conn_obj.close()

def get_date_with_maxrainfall():
    db_conn_obj=psycopg2.connect(dbname="myfiles",
                                    host="localhost",
                                    port="5432",
                                    )
    with db_conn_obj.cursor() as curs:
        query="""select distinct date 
                        from 
                            hourly_rainfall_data 
                        where 
                            rain_mm =(select MAX(rain_mm) 
                                            from 
                                                hourly_rainfall_data); """
        curs.execute(query)
        print(curs.fetchall()[0])
    db_conn_obj.close()

def identify_dryspells():
    db_conn_obj=psycopg2.connect(dbname="myfiles",
                                    host="localhost",
                                    port="5432",
                                    )
    with db_conn_obj.cursor() as curs:
        curs.execute("select date,sum(rain_mm) from hourly_rainfall_data group by date;")
        all_tuples=curs.fetchall()
    db_conn_obj.close()
    all_tuples.sort(key=lambda date:datetime.strptime(str(date[0]),'%Y-%m-%d')) # O(n logn)
    a=0
    result=[]
    for i in range(0,len(all_tuples)):     # O(n)
        if(all_tuples[i][1]==0 and a==0):
            a=1
            sdate=all_tuples[i][0]
        elif(all_tuples[i][1]==0):
            a=a+1
            edate=all_tuples[i][0]
        elif(a>5):
            a=0
            result.append((sdate,edate))
        else:
            a=0
    if(a>5):
        result.append((sdate,edate))
    for j in result:
        print(str(j[0])+"   to   "+str(j[1]))

def identify_dryspells_with_only_sql():
    db_conn_obj=psycopg2.connect(dbname="myfiles",
                                    host="localhost",
                                    port="5432",
                                    )
    with db_conn_obj.cursor() as curs:
        curs.execute("select date,sum(rain_mm) from hourly_rainfall_data group by date;")
        all_tuples=curs.fetchall()
    db_conn_obj.close()
file_path="/Users/sandeep/Documents/pocra_assignment/hourly_rainfall_jalgaon_chalisgaon_talegaon_2022.csv"
#load_csv_into_postgres(file_path)
get_date_with_maxrainfall()
identify_dryspells()

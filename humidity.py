import pymysql
def get_one_point_humidity(_db_params, _exp_id, start_time, end_time, sensor_id):
    con = pymysql.connect(host=_db_params["host"],
                              user=_db_params["user"],
                              password=_db_params["password"],
                              # db='experiment',
                              charset='utf8mb4',
                              cursorclass=pymysql.cursors.DictCursor)

    cur = con.cursor()
    cur.execute("use {}".format(_db_params["db"]))
    #cur.execute("use %s" %_db_params["db"])

    #command = 'select * from raw_data where sensor_id = 4 and time > {} ' \
        #   'and time < {}'.format(start_time, end_time)
    command = 'select * from raw_data where sensor_id = {} and time > {} ' \
              'and time < {};'.format(sensor_id, start_time, end_time)

    cur.execute(command)
    rows = cur.fetchall()
    #return (_db_params, _search_table, _exp_id)
    print(rows)

    con.close()
    humidity = [x['data'] for x in rows]
    times = [x['time'] for x in rows]

    print("humidity value")
    for data in humidity:
        print(data)

    print("time value")
    for time in times:
        print(time)

if __name__ == "__main__":

    db = {
        "host": 'localhost',
        "user": 'olga2',
        "db": 'my_db_copy',
        "password": "1234"

    }

    print("Set time start in format '2020-12-08 11:00:35'")
    t1: str = input()
    print("Set time end at the same format")
    t2: str = input()

    #print(t1)
    #print(t2)

    get_one_point_humidity (_db_params = db, _exp_id = 6, start_time=t1, end_time=t2, sensor_id = 3)

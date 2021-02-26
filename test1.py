import pymysql
def show_all_optimal_points(_db_params, _exp_id):
    con = pymysql.connect(host=_db_params["host"],
                              user=_db_params["user"],
                              password=_db_params["password"],
                              # db='experiment',
                              charset='utf8mb4',
                              cursorclass=pymysql.cursors.DictCursor)

    cur = con.cursor()
    cur.execute("use {}".format(_db_params["db"]))

    command = "select distinct(date(end_time)) as day from exp_data where " \
                  "is_finished=0 and exp_id={};".format(_exp_id)

    cur.execute(command)
    rows = cur.fetchall()
    #return (_db_params, _search_table, _exp_id)
    print(rows)

    con.close()
    days = [x['day'] for x in rows]

    print("available days:")
    for day in days:
        print(day)

if __name__ == "__main__":

    db = {
        "host": 'localhost',
        "user": 'olga2',
        "db": 'my_db_copy',
        "password": "1234"

    }
    show_all_optimal_points (_db_params = db, _exp_id = 4)

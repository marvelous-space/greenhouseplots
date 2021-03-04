import pymysql
import matplotlib.pyplot as plt
def one_point_load_raw_data(_db_params, _exp_id, show=True):
    con = pymysql.connect(host=_db_params["host"],
                          user=_db_params["user"],
                          password=_db_params["password"],
                          # db='experiment',
                          charset='utf8mb4',
                          cursorclass=pymysql.cursors.DictCursor)
    cur = con.cursor()
    cur.execute("use {}".format(_db_params["db"]))

    comm_str = "select distinct(date(end_time)) as day from exp_data where " \
              "is_finished=0 and exp_id={};".format(_exp_id)
    cur.execute(comm_str)
    rows = cur.fetchall()
    # return (_db_params, _search_table, _exp_id)
    days = [x['day'] for x in rows]
    print(days)
  #  t_start = []
   # t_stop = []
    d1 = days[0]
    d2 = days[1]
    for d in [d1, d2]:
        comm_str = "select point_id, start_time, end_time from exp_data where date(start_time) = date('{}');".format(d)
        resp = cur.execute(comm_str)
        rows = cur.fetchall()
        #point_id = rows[0]['point_id']
        points = [x['point_id'] for x in rows]
        t_start = [x['start_time'] for x in rows]
        t_stop = [x['end_time'] for x in rows]
        #t_start.append(rows[0]['start_time'])
        #t_stop.append(rows[0]['end_time'])

        print(rows)
         #print(point_id)
        print(points)
        print(t_start)
        print(t_stop)

        co2_min_array = []
        co2_max_array = []

        for (p, t1, t2) in zip(points, t_start, t_stop):
            comm_str = "select time, data from raw_data where exp_id = {} and sensor_id = {} " \
                       "and time > '{}' and time < '{}'".format(
                _exp_id, 3, t1, t2)
            resp = cur.execute(comm_str)
            co2_rows = cur.fetchall()
            print(len(co2_rows))
            co2_array = [x['data'] for x in co2_rows]
            if co2_array:
                co2_min = min(co2_array)
                co2_max = max(co2_array)
            else:
                co2_min = 0
                co2_max = 0
                print('point {} lost, t={}'.format(p, t1))
            co2_min_array.append(co2_min)
            co2_max_array.append(co2_max)

            print(co2_min)
        print(co2_min_array)

        temp_min_array = []
        temp_max_array = []

        for (p, t1, t2) in zip(points, t_start, t_stop):
            comm_str = "select time, data from raw_data where exp_id = {} and sensor_id = {} " \
                       "and time > '{}' and time < '{}'".format(
                _exp_id, 5, t1, t2)
            resp = cur.execute(comm_str)
            temp_rows = cur.fetchall()
            print(len(temp_rows))
            temp_array = [x['data'] for x in temp_rows]
            if temp_array:
                temp_min = min(temp_array)
                temp_max = max(temp_array)
            else:
                temp_min = 0
                temp_max = 0
                print('point {} lost, t={}'.format(p, t1))
            temp_min_array.append(temp_min)
            temp_max_array.append(temp_max)

            print(temp_min)
        print(temp_min_array)

        hum_min_array = []

        for (p, t1, t2) in zip(points, t_start, t_stop):
            comm_str = "select time, data from raw_data where exp_id = {} and sensor_id = {} " \
                       "and time > '{}' and time < '{}'".format(
                _exp_id, 4, t1, t2)
            resp = cur.execute(comm_str)
            hum_rows = cur.fetchall()
            print(len(hum_rows))
            hum_array = [x['data'] for x in hum_rows]
            if hum_array:
                hum_min = min(hum_array)
            else:
                hum_min = 0
                print('point {} lost, t={}'.format(p, t1))
            hum_min_array.append(hum_min)

            print(hum_min)
        print(hum_min_array)

        press_min_array = []
        press_max_array = []

        for (p, t1, t2) in zip(points, t_start, t_stop):
            comm_str = "select time, data from raw_data where exp_id = {} and sensor_id = {} " \
                       "and time > '{}' and time < '{}'".format(
                _exp_id, 2, t1, t2)
            resp = cur.execute(comm_str)
            press_rows = cur.fetchall()
            print(len(press_rows))
            press_array = [x['data'] for x in press_rows]
            if press_array:
                press_min = min(press_array)
                press_max = max(press_array)
            else:
                press_min = 0
                press_max = 0
                print('point {} lost, t={}'.format(p, t1))
            press_min_array.append(press_min)
            press_max_array.append(press_max)

            print(press_min)
        print(press_min_array)

    # строит графики
        if show:
            # 2D subplots plot
            # fig, axs = plt.subplots(4, 1, sharex=True, figsize=[12, 9])
            fig, axs = plt.subplots(4, 1, figsize=[12, 9])
            #fig.suptitle("exp={} point={} start_time={}".format(
             #   _exp_id, point_id, t_start))

            axs[0].plot(t_start, co2_min_array, "-.og", label='raw co2 data')
            axs[0].grid()
            axs[0].plot(t_start, co2_max_array, "-vr", label='raw co2 data')

            axs[1].plot(t_start, temp_min_array, "-.og", label='raw temp data')
            axs[1].grid()
            axs[1].plot(t_start, temp_max_array, "-vr", label='raw temp data')

            axs[2].plot(t_start, hum_min_array, "-.og", label='raw hum data')
            axs[2].grid()

            axs[3].plot(t_start, press_min_array, "-.og", label='raw press data')
            axs[3].grid()
            axs[3].plot(t_start, press_max_array, "-vr", label='raw press data')

            axs[0].set(ylabel='CO2, ppmv')
            axs[3].set(xlabel='time')

            plt.show()

    con.close()
    # for now we will handle one point differentiation in this callback
        # Найти point id всех точек дня => массив из них (n)
        # Подключиться к raw data
        # select time, data from raw_data where sensor_id = 3 and time
        # load co2

if __name__ == "__main__":

    db = {
        "host": 'localhost',
        "user": 'olga2',
        "db": 'my_db_copy',
        "password": "1234"

    }

    one_point_load_raw_data(_db_params = db, _exp_id = 6, show=True)
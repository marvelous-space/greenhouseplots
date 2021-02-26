import pymysql
import matplotlib.pyplot as plt
def one_point_load_raw_data(_db_params, _exp_id, point_id, show=True):
    con = pymysql.connect(host=_db_params["host"],
                          user=_db_params["user"],
                          password=_db_params["password"],
                          # db='experiment',
                          charset='utf8mb4',
                          cursorclass=pymysql.cursors.DictCursor)
    cur = con.cursor()
    cur.execute("use {}".format(_db_params["db"]))

    comm_str = "select step_id, red, white, start_time, end_time from exp_data where point_id={}".format(point_id)
    resp = cur.execute(comm_str)

    rows = cur.fetchall()
    # print(rows)
    t_start = rows[0]['start_time']
    t_stop = rows[0]['end_time']
    red = rows[0]['red']
    white = rows[0]['white']

    # for now we will handle one point differentiation in this callback
    # select time, data from raw_data where sensor_id = 3 and time
    # load co2
    comm_str = "select time, data from raw_data where exp_id = {} and sensor_id = {} " \
               "and time > '{}' and time < '{}'".format(
        _exp_id, 3, t_start, t_stop)
    resp = cur.execute(comm_str)
    co2_rows = cur.fetchall()
    print(len(co2_rows))
    co2_array = [x['data'] for x in co2_rows]
    co2_time_array = [x['time'] for x in co2_rows]

    # load si7021 hum
    comm_str = "select time, data from raw_data where exp_id = {} and sensor_id = {} " \
               "and time > '{}' and time < '{}'".format(
        _exp_id, 4, t_start, t_stop)
    resp = cur.execute(comm_str)
    hum_rows = cur.fetchall()
    print(len(hum_rows))
    hum_array = [x['data'] for x in hum_rows]
    hum_time_array = [x['time'] for x in hum_rows]

    # load si7021 temp
    comm_str = "select time, data from raw_data where exp_id = {} and sensor_id = {} " \
               "and time > '{}' and time < '{}'".format(_exp_id, 5, t_start, t_stop)
    resp = cur.execute(comm_str)
    temp_rows = cur.fetchall()
    print(len(temp_rows))
    temp_array = [x['data'] for x in temp_rows]
    temp_time_array = [x['time'] for x in temp_rows]

    # load bmp180 pressure
    comm_str = "select time, data from raw_data where exp_id = {} and sensor_id = {} " \
               "and time > '{}' and time < '{}'".format(_exp_id, 2, t_start, t_stop)
    resp = cur.execute(comm_str)
    pressure_rows = cur.fetchall()
    print(len(pressure_rows))
    pressure_array = [x['data'] for x in pressure_rows]
    pressure_time_array = [x['time'] for x in pressure_rows]
    con.close()

    if show:
        # 2D subplots plot
        # fig, axs = plt.subplots(4, 1, sharex=True, figsize=[12, 9])
        fig, axs = plt.subplots(4, 1, figsize=[12, 9])
        fig.suptitle("exp={} point={} red={} white={} start_time={}".format(
            _exp_id, point_id, red, white, t_start))

        axs[0].plot(co2_time_array, co2_array, "-g", label='raw co2 data')
        axs[0].grid()

        axs[1].plot(temp_time_array , temp_array, "-r", label='raw si7021 temp data')
        axs[1].grid()

        axs[2].plot(hum_time_array, hum_array, "-b", label='raw si7021 hum data')
        axs[2].grid()

        axs[3].plot(pressure_time_array, pressure_array, "-k", label='bmp180 pressure data')
        axs[3].grid()

        axs[0].set(ylabel='CO2, ppmv')
        axs[1].set(ylabel='Temp, C')
        axs[2].set(ylabel='Hum, %')
        axs[3].set(ylabel='Pressure, kPa')
        axs[3].set(xlabel='time')

        plt.show()

    return t_start, t_stop, red, white, co2_time_array, co2_array, \
           hum_time_array, hum_array, temp_time_array, temp_array, pressure_time_array, pressure_array


if __name__ == "__main__":

    db = {
        "host": 'localhost',
        "user": 'olga2',
        "db": 'my_db_copy',
        "password": "1234"

    }

    one_point_load_raw_data(_db_params = db, _exp_id = 5, point_id = 643, show=True)
#

    #
    # comm_str = "select point_id from exp_data where exp_id = {} and sensor_id = {} " \
    #            "and time > '{}' and time < '{}'".format(
    #     _exp_id, 3, t_start, t_stop)
    # resp = cur.execute(comm_str)
    # co2_rows = cur.fetchall()
    # print(len(co2_rows))
    # co2_array = [x['data'] for x in co2_rows]
    # co2_time_array = [x['time'] for x in co2_rows]
    # co2_min_array = min(co2_array)
    #
    # # load si7021 hum
    # comm_str = "select time, data from raw_data where exp_id = {} and sensor_id = {} " \
    #            "and time > '{}' and time < '{}'".format(
    #     _exp_id, 4, t_start, t_stop)
    # resp = cur.execute(comm_str)
    # hum_rows = cur.fetchall()
    # print(len(hum_rows))
    # hum_array = [x['data'] for x in hum_rows]
    # hum_time_array = [x['time'] for x in hum_rows]
    #
    # # load si7021 temp
    # comm_str = "select time, data from raw_data where exp_id = {} and sensor_id = {} " \
    #            "and time > '{}' and time < '{}'".format(_exp_id, 5, t_start, t_stop)
    # resp = cur.execute(comm_str)
    # temp_rows = cur.fetchall()
    # print(len(temp_rows))
    # temp_array = [x['data'] for x in temp_rows]
    # temp_time_array = [x['time'] for x in temp_rows]
    #
    # # load bmp180 pressure
    # comm_str = "select time, data from raw_data where exp_id = {} and sensor_id = {} " \
    #            "and time > '{}' and time < '{}'".format(_exp_id, 2, t_start, t_stop)
    # resp = cur.execute(comm_str)
    # pressure_rows = cur.fetchall()
    # print(len(pressure_rows))
    # pressure_array = [x['data'] for x in pressure_rows]
    # pressure_time_array = [x['time'] for x in pressure_rows]
    # con.close()
    #
    # # строит графики
    # if show:
    #     # 2D subplots plot
    #     # fig, axs = plt.subplots(4, 1, sharex=True, figsize=[12, 9])
    #     fig, axs = plt.subplots(4, 1, figsize=[12, 9])
    #     fig.suptitle("exp={} point={} red={} white={} start_time={}".format(
    #         _exp_id, point_id, red, white, t_start))
    #
    #     axs[0].plot(co2_time_array, co2_min_array, "-g", label='raw co2 data')
    #     axs[0].grid()
    #
    #     axs[1].plot(temp_time_array, temp_array, "-r", label='raw si7021 temp data')
    #     axs[1].grid()
    #
    #     axs[2].plot(hum_time_array, hum_array, "-b", label='raw si7021 hum data')
    #     axs[2].grid()
    #
    #     axs[3].plot(pressure_time_array, pressure_array, "-k", label='bmp180 pressure data')
    #     axs[3].grid()
    #
    #     axs[0].set(ylabel='CO2, ppmv')
    #     axs[1].set(ylabel='Temp, C')
    #     axs[2].set(ylabel='Hum, %')
    #     axs[3].set(ylabel='Pressure, kPa')
    #     axs[3].set(xlabel='time')
    #
    #     plt.show()
    #
    # return t_start, t_stop, red, white, co2_time_array, co2_array, \
    #        hum_time_array, hum_array, temp_time_array, temp_array, pressure_time_array, pressure_array
    #

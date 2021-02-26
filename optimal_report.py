import pymysql
import datetime
import traceback
import sys
import os
from copy import deepcopy
import time
from copy import deepcopy
from scipy import interpolate
import numpy as np
import pylab as pl
import mpl_toolkits.mplot3d.axes3d as p3
from matplotlib import cm
import matplotlib.pyplot as plt


def show_all_optimal_points(_db_params, _search_table, _exp_id):

    # lets create subdir for new exp plots

    print("\n +++++++++++++++++++++++++++++++ start preparation +++++++++++++++++++++++++++++++")

    path = os.getcwd() + '/exp_{}'.format(_exp_id)
    print(path)
    try:
        os.mkdir(path)
    except OSError as e:
        print("Creation of the directory {} failed: {}".format(path, e))

    # find all days of exp and sort them

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
    # print(rows)

    con.close()
    days = [x['day'] for x in rows]

    print("available days:")
    for day in days:
        print(day)
    # select distinct(date(end_time)) as day from exp_data where is_finished=0 and exp_id={};
    print("\n +++++++++++++++++++++++++++++++ start calculation +++++++++++++++++++++++++++++++")
    # for day in days get optimal point of f and g and picture of plot
    max_f_points = []
    min_g_points = []
    err_days = []

    for day in days:
        print("\n +++++++++++++++++++++++++++++++ day {} +++++++++++++++++++++++++++++++".format(day))
        try:
            _search_table, min_g, max_f, errs = show_optimal_point(db, search_table, exp_id, _day=day,
                                                 path=path, show=False)

            # print("========================================================")
            # print("finally we got:")
            # print(max_f)
            # print(min_g)
            max_f_points.append(max_f)
            min_g_points.append(min_g)
            # print(max_f_points)
            # print(min_g_points)

            if len(errs) > 0:
                err_days.append(day)
                print("ALARM: found errors in points:")
                for p in errs:
                    print(p)
        except Exception as e:
            exc_info = sys.exc_info()
            err_list = traceback.format_exception(*exc_info)
            print("\n ERROR, day: {} \n calculation failed: {}".format(day, err_list))
            err_days.append(day)

    print("\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("days with errors for exp{}".format(_exp_id))
    for d in err_days:
        print(d)

    print("\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("max f points for exp{}".format(_exp_id))
    for p in max_f_points:
        print(p)

    print("\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("min g points for exp{}".format(_exp_id))
    for p in min_g_points:
        print(p)

    # plot optimal points and save
    dates = [p['date'] for p in min_g_points]
    fs = [p['mean_f'] for p in min_g_points]
    red = [p['red'] for p in min_g_points]
    white = [p['white'] for p in min_g_points]
    gs = [p['mean_q'] for p in min_g_points]

    print(dates)
    print(red)
    print(white)
    print(gs)

    fig, axs = plt.subplots(4, 1, sharex=True, figsize=[12, 9])
    fig.suptitle("exp {}".format(_exp_id ))
    # cs = plt.contour(xx_new, yy_new, interp.T)
    # plt.scatter(x, y, s=area, c=z, antialiased=False, cmap=cm.coolwarm)
    # plt.subplot(311)
    axs[0].plot(dates, gs, "-og", label='trajectory of min G')
    axs[0].grid()

    axs[1].plot(dates, fs, "-ok", label='trajectory of F for min G points')
    axs[1].grid()

    # axs[0].ylabel('G')
    # plt.subplot(312)
    axs[2].plot(dates, red, "-or", label='trajectory of min G red coord')
    axs[2].grid()
    # axs[1].ylabel('red, mA')
    # plt.subplot(313)
    axs[3].plot(dates, white, "-ob", label='trajectory of min G white coord')
    axs[3].grid()
    # axs[2].ylabel('white, mA')

    axs[0].set(ylabel='G, eqv mass')
    axs[1].set(ylabel='F, ppmv/sec for full crop')
    axs[2].set(ylabel='red, mA')
    axs[3].set(ylabel='white, mA')
    axs[3].set(xlabel='date')


    # fig.xlabel('date')
    # ax.set_zlabel('dCO2/dt *-1')
    # plt.title()
    # fig.legend(_day)
    plt.savefig(path + "/{}_optimums.png".format(_exp_id))
    # pl.savefig("gradient_metaopt_5678676787656765456765.png")
    # if show:
    # plt.show()


def show_optimal_point(_db_params, search_table, _exp_id, _day, path, show=True):

    _search_table = deepcopy(search_table)

    con = pymysql.connect(host=_db_params["host"],
                          user=_db_params["user"],
                          password=_db_params["password"],
                          # db='experiment',
                          charset='utf8mb4',
                          cursorclass=pymysql.cursors.DictCursor)

    cur = con.cursor()
    cur.execute("use {}".format(_db_params["db"]))

    # in db we store at least two rows with same step_id
    # lets load them for every point in _todays_search_table and find mean Q value
    # also we will bubble search point with maximum of mean Q-value
    min_g_point = None
    max_f_point = None
    error_points = list()

    for point in _search_table:
        try:
            comm_str = "select * from exp_data where date(end_time) = date('{}')" \
                       " and exp_id={} and step_id={} and is_finished=0;".format(
                _day, _exp_id, point['number']
            )
            # print(comm_str)
            cur.execute(comm_str)
            rows = cur.fetchall()

            # lets get mean sum of q_val for that two rows
            q1 = rows[0]['q_val']
            q2 = rows[1]['q_val']

            mean_q = (q1 + q2) / 2

            f1 = rows[0]['f_val']
            f2 = rows[1]['f_val']

            mean_f = (f1 + f2) / 2

            # add that value to point as new key-value pair
            point.update({'mean_q': mean_q})
            point.update({'mean_f': mean_f})

            if not min_g_point:
                # if it is first iteration - set first point as min
                min_g_point = point
            else:
                # compare values of current point and max point
                if point['mean_q'] < min_g_point['mean_q']:
                    min_g_point = point

            if not max_f_point:
                # if it is first iteration - set first point as min
                max_f_point = point
            else:
                # compare values of current point and max point
                if point['mean_f'] > max_f_point['mean_f']:
                    max_f_point = point
        except Exception as e:
            exc_info = sys.exc_info()
            err_list = traceback.format_exception(*exc_info)
            print("\n ERROR, point: {} \n calculation failed: {} \n".format(point, err_list))
            error_points.append(point)

    # lets create 3d plot with interpolation
    # our points not lies on grid, so we have to use griddata

    min_g_point.update({'date': _day})
    max_f_point.update({'date': _day})
    print("\n min g point is : {} \n".format(min_g_point))
    print("\n min f point is : {} \n".format(max_f_point))
    print("final search table:")
    for p in _search_table:
        print(p)

    con.close()

    # prepare grid
    grid_x, grid_y = np.mgrid[10:250:1, 10:250:1]

    # prepare values on points array
    values = np.array([v['mean_q'] for v in _search_table])

    # prepare points array
    # [(x, y), ...]
    points = np.array([(v['red'], v['white']) for v in _search_table])

    grid_z0 = interpolate.griddata(points, values, (grid_x, grid_y), method='nearest')
    grid_z1 = interpolate.griddata(points, values, (grid_x, grid_y), method='linear')
    grid_z2 = interpolate.griddata(points, values, (grid_x, grid_y), method='cubic')

    # print(np.shape(grid_z0))

    # plt.subplot(221)

    # plt.imshow(func(grid_x, grid_y).T, extent=(0, 1, 0, 1), origin='lower')

    # plt.plot(points[:, 0], points[:, 1], 'k.', ms=1)

    z = np.array([v['mean_q'] for v in _search_table])
    max_z = max([v['mean_q'] for v in _search_table])
    area = np.array([(v['mean_q'] / max_z) * 250 for v in _search_table])
    z_mark = np.array([v['mean_q'] * 1.5 for v in _search_table])
    x = np.array([v['red'] for v in _search_table])
    y = np.array([v['white'] for v in _search_table])

    # plt.title('Original')

    # plt.subplot(222)
    #
    # plt.imshow(grid_z0.T)
    # plt.scatter(x, y, s=area, c=z, antialiased=False, cmap=cm.coolwarm)
    #
    # plt.title('Nearest')
    #
    # plt.subplot(223)
    #
    # plt.imshow(grid_z1.T)
    # plt.scatter(x, y, s=area, c=z, antialiased=False, cmap=cm.coolwarm)
    #
    # plt.title('Linear')
    #
    # plt.subplot(224)
    # plt.ylabel('white')
    # plt.xlabel('red')
    #
    # plt.imshow(grid_z2.T)
    # plt.scatter(x, y, s=area, c=z, antialiased=False, cmap=cm.coolwarm)
    #
    # plt.title('Cubic')
    #
    # plt.gcf().set_size_inches(6, 6)
    # # plt.savefig(path+"/{}_{}_griddata.png".format(_exp_id,_day))
    # if show:
    #      plt.show()


    # now use interp2d
    # lets generate x, y ,z separately
    z = np.array([v['mean_q'] for v in _search_table])
    z_mark = np.array([v['mean_q']*1.5 for v in _search_table])
    x = np.array([v['red'] for v in _search_table])
    y = np.array([v['white'] for v in _search_table])
    # for i in range(0, len(z)):
    #     print('x = {} y = {} z = {}'.format(x[i], y[i], z[i]))

    f = interpolate.interp2d(x, y, z, kind='cubic')

    xnew = np.arange(10, 250, 1)
    ynew = np.arange(10, 250, 1)

    interp = f(xnew, ynew)
    #
    xx_new, yy_new = np.meshgrid(xnew, ynew, indexing='ij')


    fig = plt.figure()
    cs = plt.contour(xx_new, yy_new, interp.T)
    plt.scatter(x, y, s=area, c=z, antialiased=False, cmap=cm.coolwarm)
    # pl.plot(maxes[::, 1], maxes[::, 2], "-ob", label='trajectory of maximum')
    plt.clabel(cs, fmt='%.1f')  # , colors="black")
    fig.colorbar(cs, shrink=0.5, aspect=5)

    plt.ylabel('white')
    plt.xlabel('red')
    # ax.set_zlabel('dCO2/dt *-1')
    plt.title("{} : {}".format(_exp_id , _day))
    # fig.legend(_day)
    plt.grid()
    plt.savefig(path+"/{}_{}_interp2d.png".format(_exp_id, _day))
    # pl.savefig("gradient_metaopt_5678676787656765456765.png")


    # if show:
    #     plt.show()

    return _search_table, min_g_point, max_f_point, error_points

def one_point_load_raw_data(_db_params, _exp_id, point_id, show=True):
    con = pymysql.connect(host=_db_params["host"],
                          user=_db_params["user"],
                          password=_db_params["password"],
                          # db='experiment',
                          charset='utf8mb4',
                          cursorclass=pymysql.cursors.DictCursor)
    cur = con.cursor()
    cur.execute("use {}".format(_db_params["db"]))

    comm_str = "select step_id, red, white, start_time, end_time from exp_data where point_id={}".format(
        point_id
    )
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
        exp_id, 3, t_start, t_stop)
    resp = cur.execute(comm_str)
    co2_rows = cur.fetchall()
    print(len(co2_rows))
    co2_array = [x['data'] for x in co2_rows]
    co2_time_array = [x['time'] for x in co2_rows]

    # load si7021 hum
    comm_str = "select time, data from raw_data where exp_id = {} and sensor_id = {} " \
               "and time > '{}' and time < '{}'".format(
        exp_id, 4, t_start, t_stop)
    resp = cur.execute(comm_str)
    hum_rows = cur.fetchall()
    print(len(hum_rows))
    hum_array = [x['data'] for x in hum_rows]
    hum_time_array = [x['time'] for x in hum_rows]

    # load si7021 temp
    comm_str = "select time, data from raw_data where exp_id = {} and sensor_id = {} " \
               "and time > '{}' and time < '{}'".format(
        exp_id, 5, t_start, t_stop)
    resp = cur.execute(comm_str)
    temp_rows = cur.fetchall()
    print(len(temp_rows))
    temp_array = [x['data'] for x in temp_rows]
    temp_time_array = [x['time'] for x in temp_rows]

    # load bmp180 pressure
    comm_str = "select time, data from raw_data where exp_id = {} and sensor_id = {} " \
               "and time > '{}' and time < '{}'".format(
        exp_id, 2, t_start, t_stop)
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

        pl.show()

    return t_start, t_stop, red, white, co2_time_array, co2_array, \
           hum_time_array, hum_array, temp_time_array, temp_array, pressure_time_array, pressure_array

def show_one_day(_db_params, _exp_id, day, show=True):
    con = pymysql.connect(host=_db_params["host"],
                          user=_db_params["user"],
                          password=_db_params["password"],
                          # db='experiment',
                          charset='utf8mb4',
                          cursorclass=pymysql.cursors.DictCursor)
    cur = con.cursor()
    cur.execute("use {}".format(_db_params["db"]))

    # get list of points of that day



if __name__ == "__main__":
    # db = {
    #     "host": '10.9.0.23',
    #     "user": 'remote_admin',
    #     "db": 'experiment',
    #     "password": "amstraLLa78x[$"
    # }

    # params of db
    db = {
        "host": 'localhost',
        "user": 'admin',
        "db": 'experiment_copy',
        "password": "admin"
    }
    _day = datetime.datetime.now()
    search_table = [
        {"number": 1, "red": 130, "white": 130, "finished": 0, 'f': 0, 'q': 0},
        {"number": 2, "red": 70, "white": 190, "finished": 0, 'f': 0, 'q': 0},
        {"number": 3, "red": 190, "white": 70, "finished": 0, 'f': 0, 'q': 0},
        {"number": 4, "red": 40, "white": 160, "finished": 0, 'f': 0, 'q': 0},
        {"number": 5, "red": 160, "white": 40, "finished": 0, 'f': 0, 'q': 0},
        {"number": 6, "red": 100, "white": 100, "finished": 0, 'f': 0, 'q': 0},
        {"number": 7, "red": 220, "white": 220, "finished": 0, 'f': 0, 'q': 0},
        {"number": 8, "red": 25, "white": 235, "finished": 0, 'f': 0, 'q': 0},
        {"number": 9, "red": 145, "white": 115, "finished": 0, 'f': 0, 'q': 0},
        {"number": 10, "red": 85, "white": 55, "finished": 0, 'f': 0, 'q': 0},
        {"number": 11, "red": 205, "white": 175, "finished": 0, 'f': 0, 'q': 0},
        {"number": 12, "red": 55, "white": 85, "finished": 0, 'f': 0, 'q': 0},
        {"number": 13, "red": 175, "white": 205, "finished": 0, 'f': 0, 'q': 0},
        {"number": 14, "red": 115, "white": 145, "finished": 0, 'f': 0, 'q': 0},
        {"number": 15, "red": 235, "white": 25, "finished": 0, 'f': 0, 'q': 0},
        {"number": 16, "red": 17, "white": 138, "finished": 0, 'f': 0, 'q': 0}
    ]

    exp_id = 4


    # show_all_optimal_points(db, search_table, exp_id)

    #one_point_load_raw_data(db, exp_id, point_id=1236, show=True)
    one_point_load_raw_data(db, exp_id, point_id=461, show=True)

    # exp_5_f_opts = []
    # exp_5_g_opts = []

    # path = os.getcwd() + '/exp_{}'.format(exp_id)
    # print(path)
    # try:
    #     os.mkdir(path)
    # except OSError as e:
    #     print("Creation of the directory {} failed: {}".format(path, e))

    # _, min_g, min_f, errs =show_optimal_point(db, search_table, exp_id, _day='2020-11-17',
    #                                     path=path, show=False)

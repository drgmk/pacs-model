import numpy as np
import mysql.connector
import pickle
import glob

# create table with
'''
CREATE TABLE `resolved_fitting` (
                                 `ObsId` int(11) NOT NULL,
                                 `name` varchar(50) NOT NULL DEFAULT '',
                                 `include_unres` tinyint(1) NOT NULL,
                                 `psf_obsid` int(11) NOT NULL,
                                 `resolved` tinyint(1) NOT NULL DEFAULT '0',
                                 `in_au` tinyint(1) DEFAULT NULL,
                                 `fit_ok` tinyint(1) DEFAULT NULL,
                                 `fstar_mjy` float DEFAULT NULL,
                                 `funres` float DEFAULT NULL,
                                 `fres` float DEFAULT NULL,
                                 `x0` float DEFAULT NULL,
                                 `y0` float DEFAULT NULL,
                                 `r1` float DEFAULT NULL,
                                 `r2` float DEFAULT NULL,
                                 `cosinc` float DEFAULT NULL,
                                 `theta` float DEFAULT NULL,
                                 `e_funres` float DEFAULT NULL,
                                 `e_fres` float DEFAULT NULL,
                                 `e_x0` float DEFAULT NULL,
                                 `e_y0` float DEFAULT NULL,
                                 `e_r1` float DEFAULT NULL,
                                 `e_r2` float DEFAULT NULL,
                                 `e_cosinc` float DEFAULT NULL,
                                 `e_theta` float DEFAULT NULL,
                                 PRIMARY KEY (`ObsId`,`name`,`include_unres`,`psf_obsid`)
                                 ) ENGINE=MyISAM DEFAULT CHARSET=latin1;
'''

# set up connection
try:
    cnx = mysql.connector.connect(user='grant',
                                  password='grant',
                                  host='localhost',
                                  database='herscheldb')
    cursor = cnx.cursor(buffered=True)

except mysql.connector.InterfaceError:
    print("Can't connect")

fs = glob.glob('batch_results/*/*/params.pickle')

for f in fs:
    obsid = f.split('/')[1]
    name = f.split('/')[2]
    with open(f,'rb') as file:
        r = pickle.load(file)

#    print(r)

    if r['max_likelihood'] is None:
        sql = ("INSERT INTO resolved_fitting "
               "(obsid, name, resolved, psf_obsid)"
               "VALUES ({},'{}',{},{})"
               ";".format(obsid, name, r['resolved'].real, r['psf_obsid'])
               )
    else:
        p = r['median']
        if len(p) == 7:
            p = np.insert(p, 0, 0)

        e_p = (r['upper_uncertainty'] + r['lower_uncertainty']) / 2
        if len(e_p) == 7:
            e_p = np.insert(e_p, 0, 0)

        sql = ("INSERT INTO resolved_fitting "
               "(obsid, name, resolved, include_unres, in_au, fit_ok, psf_obsid,"
               "fstar_mjy,"
               "funres, fres, x0, y0, r1, r2, cosinc, theta,"
               "e_funres, e_fres, e_x0, e_y0, e_r1, e_r2, e_cosinc, e_theta) "
               "VALUES ({},'{}',{},{},{},{},{},{},"
               "{},{},{},{},{},{},{},{},"
               "{},{},{},{},{},{},{},{})"
               ";".format(obsid, name, r['resolved'].real, r['include_unres'].real,
                          r['in_au'].real, bool(r['model_consistent']).real, r['psf_obsid'],
                          r['stellarflux'],
                          *p, *e_p)
              )

    cursor.execute(sql)
    cnx.commit()
    
cursor.close()
cnx.close()

import numpy as np
import mysql.connector
import pickle
import glob
import sys

# create table with
'''
CREATE TABLE `resolved_fitting` (
                                 `ObsId` int(11) NOT NULL,
                                 `wavelength` int(3) NOT NULL,
                                 `name` varchar(50) NOT NULL DEFAULT '',
                                 `include_unres` tinyint(1) NOT NULL,
                                 `psf_obsid` int(11) NOT NULL,
                                 `resolved` tinyint(1) NOT NULL DEFAULT '0',
                                 `in_au` tinyint(1) DEFAULT NULL,
                                 `distance float DEFAULT NULL
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
                                 PRIMARY KEY (`ObsId`,`name`,`wavelength`,`include_unres``include_alpha`,`psf_obsid`)
                                 ) ENGINE=MyISAM DEFAULT CHARSET=latin1;
'''

# get location of batch output
batch_path = sys.argv[1]

# set up connection
try:
    cnx = mysql.connector.connect(user='grant',
                                  password='grant',
                                  host='localhost',
                                  database='herscheldb',
                                  auth_plugin='mysql_native_password')
    cursor = cnx.cursor(buffered=True)

except mysql.connector.InterfaceError:
    print("Can't connect")

fs = glob.glob('{}/*/*/params.pickle'.format(batch_path))

for f in fs:

    with open(f,'rb') as file:
        r = pickle.load(file)

    obsid = f.split('/')[-3]
    name = f.split('/')[-2]


    print(r)

    if 'param_names' not in r.keys():
        sql = ("INSERT INTO resolved_fitting "
               "(obsid, name, wavelength, "
               "resolved, psf_obsid, psffit_flux, "
               "psffit_rms, pixel_rms)"
               "VALUES ({},'{}',{},{},{},{},{},{})"
               ";".format(obsid, name, r['wavelength'],
                          r['resolved'].real,
                          r['psf_obsid'], r['psffit_flux'],
                          r['psffit_rms'], r['pixel_rms'])
               )
    else:
        p = r['median']
        
        e_p = (r['upper_uncertainty'] + r['lower_uncertainty']) / 2
        if not r['include_unres']:
            p = np.insert(p, 0, 0)
            e_p = np.insert(e_p, 0, 0)

        if r['include_alpha']:
            alpha = p[-1]
            e_alpha = e_p[-1]
            p = np.delete(p, -1)
            e_p = np.delete(e_p, -1)
        else:
            alpha = r['alpha']
            e_alpha = 0

        sql = ("INSERT INTO resolved_fitting "
               "(obsid, name, wavelength, distance,"
               "resolved, include_unres, include_alpha,"
               "in_au, fit_ok, psf_obsid,"
               "psffit_flux, psffit_rms, pixel_rms,"
               "fstar_mjy, alpha, e_alpha,"
               "funres, fres, x0, y0, r1, r2, cosinc, theta,"
               "e_funres, e_fres, e_x0, e_y0, e_r1, e_r2, e_cosinc, e_theta) "
               "VALUES ({},'{}',{},{},"
               "{},{},{},"
               "{},{},{},"
               "{},{},{},"
               "{},{},{},"
               "{},{},{},{},{},{},{},{},"
               "{},{},{},{},{},{},{},{})"
               ";".format(obsid, name, r['wavelength'], r['distance'],
                          r['resolved'].real,r['include_unres'].real,
                          r['include_alpha'].real,
                          r['in_au'].real, r['fit_ok'].real, r['psf_obsid'],
                          r['psffit_flux'], r['psffit_rms'], r['pixel_rms'],
                          r['stellarflux'], alpha, e_alpha,
                          *p, *e_p)
              )

    print(sql)
    cursor.execute(sql)
    cnx.commit()
    
cursor.close()
cnx.close()

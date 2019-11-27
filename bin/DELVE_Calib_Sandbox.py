#!/usr/bin/env python

# Initial setup...
import numpy as np
import pandas as pd
from astropy.io import fits
import fitsio
from scipy import interpolate
import glob
import math

import os

import matplotlib.pyplot as plt

import astropy.coordinates as coord
import astropy.units as u

# Some db stuff...
from pandasql import sqldf
import psycopg2
import archive.database
import easyaccess as ea

# some sky position utilities...
from skymap.utils import cel2gal, gal2cel
import healpy as hp
import healpixTools


# Query BLISS db images in DELVE area...
db = archive.database.Database(dbname='db-bliss')
db.connect()
conn = db.connection

query = \
         """select f.expnum, f.ccdnum, e.band, 
                   f.path||'/'||f.filename||f.compression as filepath 
            from se_archive_info f, exposure e 
            where f.expnum = e.expnum and 
                  e.radeg is not NULL and e.decdeg is not NULL and 
                  e.radeg between 130. and 190. and
                  e.decdeg between -50. and -10. and 
                  e.glat > 10 and 
                  e.exptime > 1. and 
                  e.band in ('g','r','i','z') and 
                  f.filetype = 'fullcat'
            order by f.expnum, f.ccdnum"""
                  
df_filepaths = pd.read_sql_query(query, conn)

conn.close()

# Make all columns uppercase...
df_filepaths.columns = [x.upper() for x in df_filepaths.columns]

# Output df_filepaths...
df_filepaths.to_csv('DELVE_Calib_filepaths.csv',index=False)


# Query BLISS db exposures in DELVE area...
db = archive.database.Database(dbname='db-bliss')
db.connect()
conn = db.connection

query = \
    """select e.expnum,
            e.radeg as EXPRA, e.decdeg as EXPDEC, e.exptime, e.airmass,
            e.band, e.nite, e.mjd_obs,
            REPLACE(e.field,   ',', ' ') as field,
            REPLACE(e.object,  ',', ' ') as object,
            REPLACE(e.program, ',', ' ') as program,
            qa.*
       from exposure e, qa_summary qa
       where 
            e.expnum = qa.expnum and 
            e.radeg is not NULL and e.decdeg is not NULL and 
            e.radeg between 130. and 190. and
            e.decdeg between -50. and -10. and 
            e.glat > 10 and 
            e.exptime > 1. and 
            e.band in ('g','r','i','z') 
        order by e.expnum"""
                  
df_expinfo = pd.read_sql_query(query, conn)

conn.close()

# Make all columns uppercase...
df_expinfo.columns = [x.upper() for x in df_expinfo.columns]

# It turns out that the column 'expnum' is duplicated in the above query.
# To remove all-but-the-first of any and all duplicated columns
# (see:  https://stackoverflow.com/questions/14984119/python-pandas-remove-duplicate-columns ):
df_expinfo = df_expinfo.loc[:,~df_expinfo.columns.duplicated()]

# Output df_expinfo...
df_expinfo.to_csv('DELVE_Calib_expinfo.csv',index=False)


# Query BLISS db images in DELVE area...
db = archive.database.Database(dbname='db-bliss')
db.connect()
conn = db.connection

query = \
    """select i.expnum, i.ccdnum, i.band, 
           i.rac1, i.decc1, i.rac2, i.decc2, i.rac3, i.decc3, i.rac4, i.decc4,
           i.ra_cent, i.dec_cent, i.racmax, i.racmin, i.crossra0
       from image i
       where 
            i.band in ('g','r','i','z') and 
            i.ra_cent  between 28. and 192. and
            i.dec_cent between -52. and -8. 
       order by i.expnum, i.ccdnum"""
                  
df_imginfo = pd.read_sql_query(query, conn)

conn.close()

# Make all columns uppercase...
df_imginfo.columns = [x.upper() for x in df_imginfo.columns]

# Output df_imginfo...
df_imginfo.to_csv('DELVE_Calib_imginfo.csv',index=False)


# Merge df_imginfo with df_filepaths...
df_merge1 = pd.merge(df_imginfo, df_filepaths, \
                     on=['EXPNUM','CCDNUM'], \
                     how='inner', \
                     suffixes=('','_y')).reset_index(drop=True)

# Remove redundant columns...
to_drop = [colname for colname in df_merge1 if colname.endswith('_y')]
df_merge1.drop(to_drop, axis=1, inplace=True)

# Output df_imgfileinfo...
df_merge1.to_csv('DELVE_Calib_imgfileinfo.csv',index=False)


# Merge df_expinfo with df_imginfo...
df_merge2 = pd.merge(df_expinfo, df_imginfo, \
                     on=['EXPNUM'], \
                     how='inner', \
                     suffixes=('','_y')).reset_index(drop=True)

# Remove redundant columns...
to_drop = [colname for colname in df_merge2 if colname.endswith('_y')]
df_merge2.drop(to_drop, axis=1, inplace=True)

# Output df_expimginfo...
df_merge2.to_csv('DELVE_Calib_expimginfo.csv',index=False)


# Reset index of df_filepaths to point to (EXPNUM, CCDNUM) pairs...
#df_filepaths2 = df_filepaths.set_index(['EXPNUM','CCDNUM'])


#expnum=756865
#ccdnum=3
#inputFile = df_filepaths2.loc[expnum,ccdnum].FILEPATH
#print inputFile
#inputFileBaseName = os.path.basename(inputFile)
# Does file 'inputFile' exist?
#not os.path.isfile(inputFile)


# In[43]:

# Using suggesions from Eric Neilsen's notes page:
# http://des-ops.fnal.gov:8080/notes/neilsen/pandasExamples/pandas_examples.html#orgheadline6

npcat = fitsio.read(inputFile, columns=['NUMBER','X_IMAGE','Y_IMAGE','ALPHAWIN_J2000','DELTAWIN_J2000',
                                        'FLUX_PSF','FLUXERR_PSF','FLUX_APER','FLUXERR_APER',
                                        'CLASS_STAR','SPREAD_MODEL','SPREADERR_MODEL','FLAGS'], ext=2)
npcat = npcat.byteswap().newbyteorder()

number = npcat['NUMBER']
x_image = npcat['X_IMAGE']
y_image = npcat['Y_IMAGE']
alphawin_j2000 = npcat['ALPHAWIN_J2000']
deltawin_j2000 = npcat['DELTAWIN_J2000']
flux_psf = npcat['FLUX_PSF']
fluxerr_psf = npcat['FLUXERR_PSF']
class_star = npcat['CLASS_STAR']
spread_model = npcat['SPREAD_MODEL']
spreaderr_model = npcat['SPREADERR_MODEL']
flags = npcat['FLAGS']

flux_aper = {}
fluxerr_aper = {}
for i in range(12):
    aper_n = i+1
    #print npfluxaper['FLUX_APER'][0,i]
    flux_aper[aper_n] = npcat['FLUX_APER'][:,i]
    fluxerr_aper[aper_n] = npcat['FLUXERR_APER'][:,i]

df_cat = pd.DataFrame({'FILENAME':inputFileBaseName,                        'EXPNUM':expnum,                        'CCDNUM':ccdnum,                        'NUMBER':number,                        'X_IMAGE':x_image,                        'Y_IMAGE':y_image,                        'RA':alphawin_j2000,                        'DEC':deltawin_j2000,                        'FLUX_PSF':flux_psf,                        'FLUXERR_PSF':fluxerr_psf,                        'FLUX_APER_01':flux_aper[1],'FLUXERR_APER_01':fluxerr_aper[1],                        'FLUX_APER_02':flux_aper[2],'FLUXERR_APER_02':fluxerr_aper[2],                        'FLUX_APER_03':flux_aper[3],'FLUXERR_APER_03':fluxerr_aper[3],                        'FLUX_APER_04':flux_aper[4],'FLUXERR_APER_04':fluxerr_aper[4],                        'FLUX_APER_05':flux_aper[5],'FLUXERR_APER_05':fluxerr_aper[5],                        'FLUX_APER_06':flux_aper[6],'FLUXERR_APER_06':fluxerr_aper[6],                        'FLUX_APER_07':flux_aper[7],'FLUXERR_APER_07':fluxerr_aper[7],                        'FLUX_APER_08':flux_aper[8],'FLUXERR_APER_08':fluxerr_aper[8],                        'FLUX_APER_09':flux_aper[9],'FLUXERR_APER_09':fluxerr_aper[9],                        'FLUX_APER_10':flux_aper[10],'FLUXERR_APER_10':fluxerr_aper[10],                        'FLUX_APER_11':flux_aper[11],'FLUXERR_APER_11':fluxerr_aper[11],                        'FLUX_APER_12':flux_aper[12],'FLUXERR_APER_12':fluxerr_aper[12],                        'CLASS_STAR':class_star,                        'SPREAD_MODEL':spread_model,                        'SPREADERR_MODEL':spreaderr_model,                        'FLAGS':flags                        })

df_cat['RA_WRAP'] = df_cat['RA']
ra_wrap_mask = (df_cat.RA_WRAP >= 180.)
df_cat[ra_wrap_mask]['RA_WRAP'] = df_cat[ra_wrap_mask]['RA_WRAP'] - 360.

cols = ['FILENAME','EXPNUM','CCDNUM',
        'NUMBER','X_IMAGE','Y_IMAGE','RA_WRAP','RA','DEC',
        'FLUX_PSF','FLUXERR_PSF',
        'FLUX_APER_01','FLUX_APER_02','FLUX_APER_03','FLUX_APER_04','FLUX_APER_05','FLUX_APER_06',
        'FLUX_APER_07','FLUX_APER_08','FLUX_APER_09','FLUX_APER_10','FLUX_APER_11','FLUX_APER_12',
        'FLUXERR_APER_01','FLUXERR_APER_02','FLUXERR_APER_03','FLUXERR_APER_04','FLUXERR_APER_05','FLUXERR_APER_06',
        'FLUXERR_APER_07','FLUXERR_APER_08','FLUXERR_APER_09','FLUXERR_APER_10','FLUXERR_APER_11','FLUXERR_APER_12',
        'CLASS_STAR','SPREAD_MODEL','SPREADERR_MODEL','FLAGS']
df_cat = df_cat[cols]


# In[44]:

df_cat.head(10)


# In[45]:

df_cat.iloc[27]


# In[46]:

fileName = 'xxx/yyy/cats.csv'
os.path.basename(fileName)


# In[47]:

#df_gaiadr2 = pd.read_csv('StarClusterPhotom/gaiadr2_around_new_bliss_starcluster.csv')
#df_gaiadr2.head(5)


# In[48]:

#df_gaiadr2.columns = [x.upper() for x in df_gaiadr2.columns]


# In[49]:

#df_gaiadr2.head(5)


# ## Find all the nside=8 nested healpixels for a given EXPNUM...

# In[50]:

# df_merge2 is a merge between df_expinfo and df_imginfo
df_merge2.head(10)


# In[51]:

expnum=756865


# In[52]:

df_merge2.loc[0]


# In[53]:

df_merge2['HPX8_1'] = healpixTools.getipix(8, df_merge2['RAC1'], df_merge2['DECC1'])
df_merge2['HPX8_2'] = healpixTools.getipix(8, df_merge2['RAC2'], df_merge2['DECC2'])
df_merge2['HPX8_3'] = healpixTools.getipix(8, df_merge2['RAC3'], df_merge2['DECC3'])
df_merge2['HPX8_4'] = healpixTools.getipix(8, df_merge2['RAC4'], df_merge2['DECC4'])
df_merge2['HPX8_CENT'] = healpixTools.getipix(8, df_merge2['RA_CENT'], df_merge2['DEC_CENT'])


# In[54]:

df_merge2[(df_merge2.EXPNUM==162966)].HPX8_1


# In[55]:

hpx8_array_1 = np.sort(df_merge2[(df_merge2.EXPNUM==162966)].HPX8_1.unique())
hpx8_array_2 = np.sort(df_merge2[(df_merge2.EXPNUM==162966)].HPX8_2.unique())
hpx8_array_3 = np.sort(df_merge2[(df_merge2.EXPNUM==162966)].HPX8_3.unique())
hpx8_array_4 = np.sort(df_merge2[(df_merge2.EXPNUM==162966)].HPX8_4.unique())
hpx8_array_CENT = np.sort(df_merge2[(df_merge2.EXPNUM==162966)].HPX8_CENT.unique())


# In[56]:

print hpx8_array_1
print hpx8_array_2
print hpx8_array_3
print hpx8_array_4
print hpx8_array_CENT


# In[57]:

hpx8_array = np.concatenate((hpx8_array_1,hpx8_array_2,hpx8_array_3,hpx8_array_4,hpx8_array_CENT), axis=None)
hpx8_array


# In[58]:

hpx8_array = np.sort(np.unique(hpx8_array))
hpx8_array


# In[59]:

healpixTools.getipix(8, 15.0, -30.0)


# In[ ]:




# ## ATLAS-REFCAT2 --> DES DR1 Transformation Equations
# 
# * ATLAS-REFCAT2 g,r,i,z are PanSTARRS AB mags.
# 
# * Use ATLAS-REFCAT2 16.0 < i < 20.0 for the transformation equations.
# 
# * Use DES DR1 data centered at RA=17.0deg (+/-2.5deg), DEC=-30.0 (+/-2.5deg).
# 
# * Use ATLAS-REFCAT2 data in nested HPX8 pixel 558.
# 
# <pre>
# g_des = g_ps + 0.0994*(g-r)_ps - 0.0076             -0.2 < (g-r)_ps <= 1.2
# 
# r_des = r_ps - 0.1335*(g-r)_ps + 0.0189             -0.2 < (g-r)_ps <= 1.2
#    
# i_des = i_ps - 0.3407*(i-z)_ps + 0.0026             -0.2 < (i-z)_ps <= 0.3
# 
# z_des = z_ps - 0.2575*(i-z)_ps - 0.0074             -0.2 < (i-z)_ps <= 0.3
# 
# Y_des = z_ps - 0.6032*(i-z)_ps + 0.0185             -0.2 < (i-z)_ps <= 0.3
# </pre>
# 

# 

# In[ ]:




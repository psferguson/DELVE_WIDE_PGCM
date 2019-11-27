#!/usr/bin/env python

# Initial setup...
import numpy as np
import pandas as pd
import archive.database
import os

# Query BLISS db image filepaths in delve-deep program...
print 'Query BLISS db image filepaths in delve-deep program...'
db = archive.database.Database(dbname='db-bliss')
db.connect()
conn = db.connection

query = \
         """select f.expnum, f.ccdnum, e.band, 
                   f.path||'/'||f.filename||f.compression as filepath 
            from se_archive_info f, exposure e 
            where f.expnum = e.expnum and
                  (e.exptime > 290. and e.exptime < 350.) and 
                  (e.decdeg < -50.) and
                  (e.radeg > 105. and e.radeg < 130.) and 
                  (e.band = 'i') and 
                  f.filetype = 'fullcat'
            order by f.expnum, f.ccdnum"""
                  
df_filepaths = pd.read_sql_query(query, conn)

conn.close()

# Make all columns uppercase...
df_filepaths.columns = [x.upper() for x in df_filepaths.columns]

# Output df_filepaths...
df_filepaths.to_csv('DELVE_Calib_filepaths_fnal.csv',index=False)


# Query BLISS db exposures in delve-deep program...
print 'Query BLISS db exposures in delve-deep program...'
db = archive.database.Database(dbname='db-bliss')
db.connect()
conn = db.connection

query = \
    """select e.expnum,
            e.radeg as EXPRA, e.decdeg as EXPDEC, e.exptime, e.airmass,
            e.band, e.nite, e.mjd_obs,
            REPLACE(e.field,   ',', ' ') as field,
            REPLACE(e.object,  ',', ' ') as object,
            REPLACE(e.program, ',', ' ') as program
       from exposure e
       where 
            (e.exptime > 290. and e.exptime < 350.) and 
            (e.decdeg < -50.) and
            (e.radeg > 105. and e.radeg < 130.) and 
            (e.band = 'i')
    """
                  
df_expinfo = pd.read_sql_query(query, conn)

conn.close()

# Make all columns uppercase...
df_expinfo.columns = [x.upper() for x in df_expinfo.columns]

# It turns out that the column 'expnum' is duplicated in the above query.
# To remove all-but-the-first of any and all duplicated columns
# (see:  https://stackoverflow.com/questions/14984119/python-pandas-remove-duplicate-columns ):
df_expinfo = df_expinfo.loc[:,~df_expinfo.columns.duplicated()]

# Output df_expinfo...
df_expinfo.to_csv('DELVE_Calib_expinfo_fnal.csv',index=False)


# Query BLISS db images in delve-deep program...
print 'Query BLISS db images in delve-deep program...'
db = archive.database.Database(dbname='db-bliss')
db.connect()
conn = db.connection

query = \
    """select i.expnum, i.ccdnum, i.band, 
              i.rac1, i.decc1, i.rac2, i.decc2, i.rac3, i.decc3, i.rac4, i.decc4,
              i.ra_cent, i.dec_cent, i.racmax, i.racmin, i.crossra0
       from image i, exposure e
       where i.expnum = e.expnum and
             (e.exptime > 290. and e.exptime < 350.) and 
             (e.decdeg < -50.) and
             (e.radeg > 105. and e.radeg < 130.) and 
             (e.band = 'i')
       order by i.ccdnum"""
                  
df_imginfo = pd.read_sql_query(query, conn)

conn.close()

# Make all columns uppercase...
df_imginfo.columns = [x.upper() for x in df_imginfo.columns]

# Output df_imginfo...
df_imginfo.to_csv('DELVE_Calib_imginfo_fnal.csv',index=False)


# Merge df_imginfo with df_filepaths...
print 'Merge df_imginfo with df_filepaths...'
df_imgfileinfo = pd.merge(df_imginfo, df_filepaths, \
                              on=['EXPNUM','CCDNUM'], \
                              how='inner', \
                              suffixes=('','_y')).reset_index(drop=True)

# Remove any redundant columns...
to_drop = [colname for colname in df_imgfileinfo if colname.endswith('_y')]
df_imgfileinfo.drop(to_drop, axis=1, inplace=True)

# Output df_imgfileinfo...
df_imgfileinfo.to_csv('DELVE_Calib_imgfileinfo_fnal.csv',index=False)


# Merge df_expinfo with df_imgfileinfo...
print 'Merge df_expinfo with df_imgfileinfo...'
df_expimgfileinfo = pd.merge(df_expinfo, df_imgfileinfo, \
                                 on=['EXPNUM'], \
                                 how='inner', \
                                 suffixes=('','_y')).reset_index(drop=True)

# Remove any redundant columns...
to_drop = [colname for colname in df_expimgfileinfo if colname.endswith('_y')]
df_expimgfileinfo.drop(to_drop, axis=1, inplace=True)

# Output df_expimginfo...
df_expimgfileinfo.to_csv('DELVE_Calib_expimgfileinfo_fnal.csv',index=False)


# If it does not alreayd exist, create a subdiretory called Downloads...
if not os.path.exists('Downloads'):
    os.makedirs('Downloads')

# Loop through df_expimgfileinfo, copying the delve-deep 
#  files into the Downloads sub-directory...
# (Could do symbolic links, but better to copy, just to 
#  be safe and not accidentally delete or modify the 
#  original files...)
for index, row in df_expimgfileinfo.iterrows():
    print row['EXPNUM'], row['FILEPATH']
    if os.path.exists(row['FILEPATH']):
        cmd="""cp -p %s ./Downloads/""" % (row['FILEPATH'])
        os.system(cmd)
    else:
        """WARNING:  %s does not exist!  Skipping...""" % (row['FILEPATH'])


print 'Finis!'


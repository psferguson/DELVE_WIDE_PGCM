#!/usr/bin/env python

# Initial setup...
import numpy as np
import pandas as pd
import easyaccess as ea


# Query image filepaths from NCSA DELVE db...
print 'Query image filepaths from NCSA DELVE db...'

# The query...
#  See:  https://sites.google.com/view/delve-survey/data?pli=1&authuser=1
query = \
    """
    with 
    images as 
        (select i.expnum, i.ccdnum, i.band, 
                racmin, racmax, deccmin, deccmax, 
                'http://decade.ncsa.illinois.edu/deca_archive/'||fai.path||'/'||i.filename||fai.compression as ilink 
         from decade.proctag t, decade.image i, decade.file_archive_info fai 
         where t.tag='DELVE' and 
               t.pfw_attempt_id=i.pfw_attempt_id and 
               i.filetype='red_immask' and 
               i.filename=fai.filename 
         order by expnum, ccdnum),  
    catalogs as 
        (select c.expnum, c.ccdnum, 
                'http://decade.ncsa.illinois.edu/deca_archive/'||fai.path||'/'||c.filename||fai.compression as clink 
         from decade.proctag t, decade.catalog c, decade.file_archive_info fai 
         where t.tag='DELVE' and 
               t.pfw_attempt_id=c.pfw_attempt_id and 
               c.filetype='cat_finalcut' and 
               c.filename=fai.filename 
         order by expnum, ccdnum) 
    select images.expnum as expnum, images.ccdnum as ccdnum, 
           band, racmin, racmax, deccmin, deccmin, ilink, clink 
    from images, catalogs 
    where images.expnum = catalogs.expnum and 
          images.ccdnum = catalogs.ccdnum
    order by expnum, ccdnum
    """

# Query NCSA DELVE db...
connection=ea.connect('delve')
df_filepaths = connection.query_to_pandas(query)
connection.close()

# Make all columns uppercase...
df_filepaths.columns = [x.upper() for x in df_filepaths.columns]

# Output df_filepaths...
df_filepaths.to_csv('DELVE_Calib_filepaths_ncsa.csv',index=False)


# Query exposure info from NCSA DELVE db...
print 'Query exposure info from NCSA DELVE db...'

# The query...
#  Based on the NCSA DELVE exposure query at
#  https://sites.google.com/view/delve-survey/data?pli=1&authuser=1
query = \
    """
    select e.expnum, e.radeg as EXPRA, e.decdeg as EXPDEC, e.exptime, e.airmass,
           e.band, e.nite, e.mjd_obs, 
           REPLACE(e.field,   ',', ' ') as field,
           REPLACE(e.object,  ',', ' ') as object,
           REPLACE(e.program, ',', ' ') as program,
           concat('/deca_archive/',a.archive_path) as path, 
           f.*
    from exposure e, finalcut_eval f, proctag t, pfw_attempt a 
    where t.pfw_attempt_id=f.pfw_attempt_id and 
          t.pfw_attempt_id=a.id and 
          f.expnum=e.expnum and 
          t.tag='DELVE'
    order by e.expnum
    """


# Query NCSA DELVE db...
connection=ea.connect('delve')
df_expinfo = connection.query_to_pandas(query)
connection.close()

# Make all columns uppercase...
df_expinfo.columns = [x.upper() for x in df_expinfo.columns]

# It turns out that the column 'expnum' is duplicated in the above query.
# To remove all-but-the-first of any and all duplicated columns
# (see:  https://stackoverflow.com/questions/14984119/python-pandas-remove-duplicate-columns ):
df_expinfo = df_expinfo.loc[:,~df_expinfo.columns.duplicated()]

# Output df_expinfo...
df_expinfo.to_csv('DELVE_Calib_expinfo_ncsa.csv',index=False)


# Query image info from NCSA DELVE db...
print 'Query image info from NCSA DELVE db...'

# The query...
#  Based on the NCSA DELVE exposure query at
#  https://sites.google.com/view/delve-survey/data?pli=1&authuser=1
query = \
    """
    select i.expnum, i.ccdnum, i.band, 
           i.rac1, i.decc1, i.rac2, i.decc2, i.rac3, i.decc3, i.rac4, i.decc4,
           i.ra_cent, i.dec_cent, i.racmax, i.racmin, i.crossra0           
    from decade.proctag t, decade.image i
    where t.tag='DELVE' and 
          t.pfw_attempt_id=i.pfw_attempt_id and 
          i.filetype='red_immask'
    order by expnum, ccdnum 
    """

# Query NCSA DELVE db...
connection=ea.connect('delve')
df_imginfo = connection.query_to_pandas(query)
connection.close()

# Make all columns uppercase...
df_imginfo.columns = [x.upper() for x in df_imginfo.columns]

# Output df_imginfo...
df_imginfo.to_csv('DELVE_Calib_imginfo_ncsa.csv',index=False)


# Merge df_imginfo with df_filepaths...
# Note:  this is mostly redundant with the NCSA DELVE db query
#        for filepaths.  Include it anyway, for similarity with
#        FNAL DELVE db query code...
print 'Merge df_imginfo with df_filepaths...'
df_imgfileinfo = pd.merge(df_imginfo, df_filepaths, \
                              on=['EXPNUM','CCDNUM'], \
                              how='inner', \
                              suffixes=('','_y')).reset_index(drop=True)

# Remove any redundant columns...
to_drop = [colname for colname in df_imgfileinfo if colname.endswith('_y')]
df_imgfileinfo.drop(to_drop, axis=1, inplace=True)

# Output df_imgfileinfo...
df_imgfileinfo.to_csv('DELVE_Calib_imgfileinfo_ncsa.csv',index=False)


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
df_expimgfileinfo.to_csv('DELVE_Calib_expimgfileinfo_ncsa.csv',index=False)


print 'Finis!'


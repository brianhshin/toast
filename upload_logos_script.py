"""
@author: Brian S.
@date: 12.28.17

Parses data from Crunchbase, Orb US, Orb Foreign, and Zoom data to upload the logos by domain to Cloudinary.

"""
import os, sys
import json
import tldextract
import pandas as pd
import urllib3
import requests
import datetime
import numpy as np
import argparse

from cloudinary.uploader import upload
from cloudinary.utils import cloudinary_url
from cloudinary.api import delete_resources_by_tag, resources_by_tag, update
from joblib import Parallel, delayed

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# for testing
from itertools import islice

######################################################

# changing directory to project folder
os.chdir('/Users/brian.shin/Documents/files/cloudinary_logos_us26181/source_files/')
current_path = os.getcwd()
if os.path.exists(current_path):
    print('path:', current_path)
else:
    print('path not found.')

# set paths for all file variables/function inputs
cb_file = 'cb_deduped.csv'
orb_us_file = 'orb_us.csv'
orb_foreign_file = 'orb_foreign.csv'
zoom_file = 'zoom_companylogodump.csv'

######################################################

# gets data from crunchbase file
def get_cb_data(cb_file):

    # make sure the file exists
    if os.path.exists(cb_file):
        print('file:', cb_file)
    else:
        print('file not found.')

    # read the orb US file, parse the company name, domain, logo, n_relationships (as a metric of best company w/ that domain)
    cb = pd.read_csv(cb_file, encoding='latin')
    cb = cb[['name', 'domain', 'logo_url', 'n_relationships']]
    cb['n_relationships'] = cb['n_relationships'].astype(float)
    cb = cb.sort_values(['n_relationships'], ascending=False).groupby('domain').head(1).reset_index()

    counter = 0
    # create empty list to populate with orb data
    cb_data = []

    # for each row/company, convert data to string
    for index, row in cb.iterrows():
        raw_domain = str(row['domain'])
        extracted = tldextract.extract(raw_domain)
        row_domain = str("{}.{}".format(extracted.domain, extracted.suffix)).lower()
        row_logo_url = str(row['logo_url'])
        row_name = str(row['name'])
        row_n_relationships = str(row['n_relationships'])

        # excludes companies without domains and logos
        if row_domain != 'nan' and row_logo_url != 'nan' and len(str(row_domain)) > 0 and len(str(row_logo_url)) > 0:
            counter = counter + 1
            obj = {"logo": row_logo_url,
                   "domain": row_domain,
                   "row_name": row_name,
                   "row_n_relationships": row_n_relationships}

            cb_data.append(obj)

    print('cb counted:', len(cb_data), 'domains with logos.')
    return cb_data

######################################################

# gets data from orb US file
def get_orb_us_data(orb_us_file):

    # make sure the file exists
    if os.path.exists(orb_us_file):
        print('file:', orb_us_file)
    else:
        print('file not found.')

    # read the orb US file and only pull the domain and logo_url
    orb_us = pd.read_csv(orb_us_file, encoding='latin')
    orb_us = orb_us[['name', 'webdomain', 'logo_url', 'search_rank']]
    orb_us = orb_us.sort_values(['search_rank'], ascending=False).groupby('webdomain').head(1).reset_index()

    counter = 0
    # create empty list to populate with orb data
    orb_us_data = []

    # for each row/company, convert data to string
    for index, row in orb_us.iterrows():
        raw_domain = str(row['webdomain'])
        extracted = tldextract.extract(raw_domain)
        row_domain = str("{}.{}".format(extracted.domain, extracted.suffix)).lower()
        row_logo_url = str(row['logo_url'])
        row_name = str(row['name'])
        row_search_rank = str(row['search_rank'])

        # excludes companies without domains and logos
        if row_domain != 'nan' and row_logo_url != 'nan' and len(str(row_domain)) > 0 and len(str(row_logo_url)) > 0:
            counter = counter + 1
            obj = {"logo": row_logo_url,
                   "domain": row_domain,
                   "row_name": row_name,
                   "search_rank": row_search_rank}

            orb_us_data.append(obj)

    print('orb_us counted:', len(orb_us_data), 'domains with logos.')
    return orb_us_data

######################################################

def get_orb_foreign_data(orb_foreign_file):

    # make sure the file exists
    if os.path.exists(orb_foreign_file):
        print('file:', orb_foreign_file)
    else:
        print('file not found.')

    # read the orb US file and only pull the domain and logo_url
    orb_foreign = pd.read_csv(orb_foreign_file, encoding='latin')
    orb_foreign = orb_foreign[['name', 'webdomain', 'logo_url', 'search_rank']]
    orb_foreign = orb_foreign.sort_values(['search_rank'], ascending=False).groupby('webdomain').head(1).reset_index()

    counter = 0
    # create empty list to populate with orb data
    orb_foreign_data = []

    # for each row/company, convert data to string
    for index, row in orb_foreign.iterrows():
        raw_domain = str(row['webdomain'])
        extracted = tldextract.extract(raw_domain)
        row_domain = str("{}.{}".format(extracted.domain, extracted.suffix)).lower()
        row_logo_url = str(row['logo_url'])
        row_name = str(row['name'])
        row_search_rank = str(row['search_rank'])

        # excludes companies without domains and logos
        if row_domain != 'nan' and row_logo_url != 'nan' and len(str(row_domain)) > 0 and len(str(row_logo_url)) > 0:
            counter = counter + 1
            obj = {"logo": row_logo_url,
                   "domain": row_domain,
                   "row_name": row_name,
                   "search_rank": row_search_rank}

            orb_foreign_data.append(obj)

    print('orb_foreign counted:', len(orb_foreign_data), 'domains with logos.')
    return orb_foreign_data
######################################################

def get_zoom_data(zoom_file):

    if os.path.exists(zoom_file):
        print('file:', zoom_file)
    else:
        print('file not found.')

    zoom = pd.read_csv(zoom_file, encoding='latin')
    zoom = zoom[['d_url', 'd_logo']]
    zoom = zoom.groupby('d_url').head(1).reset_index()

    zoom_data = []

    for index, row in zoom.iterrows():
        raw_domain = str(row['d_url'])
        extracted = tldextract.extract(raw_domain)
        row_domain = str("{}.{}".format(extracted.domain, extracted.suffix)).lower()
        row_logo_url = str(row['d_logo'])

        if row_domain != 'nan' and row_logo_url != 'nan' and len(str(row_domain)) > 0 and len(str(row_logo_url)) > 0:
            obj = {"logo": row_logo_url,
                   "domain": row_domain}
            zoom_data.append(obj)


    print('zoom counted:', len(zoom_data), 'domains with logos.')
    return zoom_data

######################################################

# was using for testing
def dump_response(response):
    print("Upload response:")
    for key in sorted(response.keys()):
        print("  %s: %s" % (key, response[key]))

######################################################
# combines all the data from crunchbase, orb, and zoom into a single dataframe
def combine_data(cb_file, orb_us_file, orb_foreign_file, zoom_file):

    cb_data = get_cb_data(cb_file)
    orb_us_data = get_orb_us_data(orb_us_file)
    orb_foreign_data = get_orb_foreign_data(orb_foreign_file)
    zoom_data = get_zoom_data(zoom_file)
    print('\ntotal domains with logos:', len(cb_data)+len(orb_us_data)+len(orb_foreign_data)+len(zoom_data))

    cb_df = pd.DataFrame(cb_data)
    orb_us_df = pd.DataFrame(orb_us_data)
    orb_foreign_df = pd.DataFrame(orb_foreign_data)
    zoom_df = pd.DataFrame(zoom_data).drop_duplicates(subset=['domain'])

    # takes the top ranked logo grouped by domain, drops indexes to avoid faulty merging
    df = pd.merge(cb_df, orb_us_df, on='domain', how = 'outer').groupby('domain').head(1).reset_index().drop(['index'], 1)
    df2 = pd.merge(df, orb_foreign_df, on= 'domain', how= 'outer').groupby('domain').head(1).reset_index().drop(['index'], 1)
    data = pd.merge(df2, zoom_df, on= 'domain', how= 'outer').groupby('domain').head(1).reset_index().drop(['index'], 1)
    data.columns = ['domain', 'cb_logo', 'cb_n_relationships', 'cb_name', 'orb_us_logo', 'orb_us_name', 'orb_us_search_rank', 'orb_foreign_logo', 'orb_foreign_name', 'orb_foreign_search_rank', 'zoom_logo']
    print(data.shape)
    #data.head(2)
    return data

######################################################

now = datetime.datetime.now()
date = "{}".format(now.day, now.month, now.year)
now.strftime("%Y-%m-%d")

# uploads files
def upload_files(i):

    # try uploading crunchbase logos first
    try:
        # i = the row number from the compiled dataframe
        domain = data.iloc[i]['domain']
        logo_url = data.iloc[i]['cb_logo']
        # cloudinary's upload function using the api for the crunchbase logo of the dataframe's row- overwrite=False to avoid writing over good logos
        upload(logo_url,
               public_id=domain,
               overwrite=False,
               invalidate=True,
               discard_original_filename=True,
               tags = [domain,"CompanyLogo",date],
               api_key="259763555666576",
               api_secret="-duLrHQ-ujQb6nAiUKs9MV-3j2Y",
               cloud_name="zoominfo-com")

        print('{0} {1:50} {2:20} {3}'.format(i, 'domain: '+domain, 'source: crunchbase', 'logo: '+logo_url))
        #print(i, 'domain: '+domain, 'source: crunchbase', 'logo: '+logo_url)
        good_upload = {'domain':domain, 'logo':logo_url, 'source':'crunchbase'}


    # when there's an error- we don't have a crunchbase logo for the company or the logo url can't be reached
    except Exception as ex:
        print(i, domain,'cb_logo:', logo_url, 'empty or down.')

        # try uploading orb_us logos next
        try:
            domain = data.iloc[i]['domain']
            logo_url = data.iloc[i]['orb_us_logo']
            upload(logo_url,
                   public_id=domain,
                   overwrite=False,
                   invalidate=True,
                   discard_original_filename=True,
                   tags = [domain,"CompanyLogo",date],
                   api_key="259763555666576",
                   api_secret="-duLrHQ-ujQb6nAiUKs9MV-3j2Y",
                   cloud_name="zoominfo-com")

            print('{0} {1:50} {2:20} {3}'.format(i, 'domain: '+domain, 'source: orb_us', 'logo: '+logo_url))
            good_upload = {'domain':domain, 'logo':logo_url, 'source':'orb_us'}

        except Exception as ex:
            print(i, domain,'orb_us_logo:', logo_url, 'empty or down.')

            # try uploading orb_foreign logos next
            try:
                domain = data.iloc[i]['domain']
                logo_url = data.iloc[i]['orb_foreign_logo']
                upload(logo_url,
                       public_id=domain,
                       overwrite=False,
                       invalidate=True,
                       discard_original_filename=True,
                       tags = [domain,"CompanyLogo",date],
                       api_key="259763555666576",
                       api_secret="-duLrHQ-ujQb6nAiUKs9MV-3j2Y",
                       cloud_name="zoominfo-com")

                print('{0} {1:50} {2:20} {3}'.format(i, 'domain: '+domain, 'source: orb_foreign', 'logo: '+logo_url))
                good_upload = {'domain':domain, 'logo':logo_url, 'source':'orb_foreign'}

            except Exception as ex:
                print(i, domain,'orb_foreign_logo:', logo_url, 'empty or down.')

                # upload zoom logos last
                try:
                    domain = data.iloc[i]['domain']
                    logo_url = data.iloc[i]['zoom_logo']
                    upload(logo_url,
                           public_id=domain,
                           overwrite=False,
                           invalidate=True,
                           discard_original_filename=True,
                           tags = [domain,"CompanyLogo",date],
                           api_key="259763555666576",
                           api_secret="-duLrHQ-ujQb6nAiUKs9MV-3j2Y",
                           cloud_name="zoominfo-com")


                    print('{0} {1:50} {2:20} {3}'.format(i, 'domain: '+domain, 'source: zoom', 'logo: '+logo_url))
                    good_upload = {'domain':domain, 'logo':logo_url, 'source':'zoom'}

                except Exception as ex:
                    print('{0} {1:50} {2:20} {3}'.format(i, 'domain: '+domain, 'source: all', 'logo: empty or down'))
                    good_upload = {'domain':'None', 'logo':'None', 'source':'None'}

    return good_upload

######################################################
start_time = datetime.datetime.now()

data = pd.read_csv('all_logos.csv', encoding='latin')
#data = combine_data(cb_file, orb_us_file, orb_foreign_file, zoom_file)
# can continue where you leave off by changing first value of range tuple
inputs = range(4613103, len(data))
# number of threads running
num_cores = 13
#num_cores = 1

good_uploads = Parallel(n_jobs=num_cores)(delayed(upload_files)(i) for i in inputs)
good_uploads_df = pd.DataFrame(good_uploads).dropna(subset=['domain']).to_csv('successfully_uploaded_cloudinary_logos_'+start_time+'.csv')

time = (datetime.datetime.now() - start_time)
print("--- {} ---".format(datetime.timedelta(seconds=time.seconds)))

######################################################

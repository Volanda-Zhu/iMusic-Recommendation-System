#!/usr/bin/env python
# coding: utf-8

# In[36]:


#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import h5py
import os
import boto3 
import tempfile
import pandas as pd
from getinfo import *
import csv
import smart_open


# In[37]:


# If you have the data already in s3, this function lists all the files in s3 that start with the given prefix
def get_prefixes(letter, bucket='millionsongs605', prefix='data/'):
    # In order to run with multiple threads/machines at a time, the prefix could be set to different things,
    # to make sure there is no overlap. For example, 'million-song/data/A', 'million-song/data/B', ...
    prefix = prefix+letter+"/"
    s3 = boto3.client('s3')

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    prefixes = [content['Key'] for content in response['Contents']]

    while response['IsTruncated']:
        response = s3.list_objects_v2(Bucket=bucket, 
                                      Prefix=prefix, 
                                      ContinuationToken=response['NextContinuationToken'])
        prefixes.extend([content['Key'] for content in response['Contents']])
        
    return prefixes



# In[39]:


def process_h5_file(h5_file):
    """Do the processing of what fields you'll use here.
     For example, to get the artist familiarity, refer to:
     https://github.com/tbertinmahieux/MSongsDB/blob/master/PythonSrc/hdf5_getters.py
     So we see that it does h5.root.metadata.songs.cols.artist_familiarity[songidx] 
     and it would translate to:
       num_songs = len(file['metadata']['songs'])
       file['metadata']['songs'][:num_songs]['artist_familiarity']
     Since there is one song per file, it simplifies to:
       file['metadata']['songs'][:1]['artist_familiarity']
     I recommend downloading one file, opening it with h5py, and explore/practice
     To see the datatype and shape:
     http://millionsongdataset.com/pages/field-list/
     http://millionsongdataset.com/pages/example-track-description/
     """
#     song_num = len(h5_file['metadata']['songs'])
#     result = []
#     for songidx in range(song_num):
    song_info = []

    song_info.append(float(h5_file['metadata']['songs'][:1]['artist_familiarity'][0]))
    song_info.append(float(h5_file['metadata']['songs'][:1]['artist_hotttnesss'][0]))
    song_info.append(str(h5_file['metadata']['songs'][:1]['artist_id'][0].decode("utf-8")))
    song_info.append(str(h5_file['metadata']['songs'][:1]['artist_location'][0].decode("utf-8")))
    song_info.append([x.decode("utf-8") for x in h5_file['musicbrainz']['artist_mbtags'][h5_file['musicbrainz']['songs'][:1]['idx_artist_mbtags'][0]:].tolist()])
    song_info.append([x.decode("utf-8") for x in h5_file['musicbrainz']['artist_mbtags_count'][h5_file['musicbrainz']['songs'][:1]['idx_artist_mbtags'][0]:].tolist()])
    song_info.append(str(h5_file['metadata']['songs'][:1]['artist_name'][0].decode("utf-8")))
    song_info.append([x.decode("utf-8") for x in h5_file['metadata']['artist_terms'][h5_file['metadata']['songs'][:1]['idx_artist_terms'][0]:].tolist()])
    song_info.append(h5_file['metadata']['artist_terms_freq'][h5_file['metadata']['songs'][:1]['idx_artist_terms'][0]:].tolist())
    song_info.append(h5_file['metadata']['artist_terms_weight'][h5_file['metadata']['songs'][:1]['idx_artist_terms'][0]:].tolist())
    song_info.append(float(h5_file['analysis']['songs'][:1]['danceability'][0]))
    song_info.append(float(h5_file['analysis']['songs'][:1]['duration'][0]))
    song_info.append(float(h5_file['analysis']['songs'][:1]['end_of_fade_in'][0]))
    song_info.append(float(h5_file['analysis']['songs'][:1]['energy'][0]))
    song_info.append(float(h5_file['analysis']['songs'][:1]['key'][0]))
    song_info.append(float(h5_file['analysis']['songs'][:1]['key_confidence'][0]))
    song_info.append(float(h5_file['analysis']['songs'][:1]['loudness'][0]))
    song_info.append(float(h5_file['analysis']['songs'][:1]['mode'][0]))
    song_info.append(float(h5_file['analysis']['songs'][:1]['mode_confidence'][0]))
    song_info.append(str(h5_file['metadata']['songs'][:1]['release'][0].decode("utf-8")))
    song_info.append(h5_file['analysis']['segments_confidence'][h5_file['analysis']['songs'][:1]['idx_segments_confidence'][0]:].tolist())
    song_info.append(h5_file['analysis']['segments_loudness_max'][h5_file['analysis']['songs'][:1]['idx_segments_loudness_max'][0]:].tolist())
    song_info.append(h5_file['analysis']['segments_loudness_max_time'][h5_file['analysis']['songs'][:1]['idx_segments_loudness_max_time'][0]:].tolist())
    song_info.append(h5_file['analysis']['segments_pitches'][h5_file['analysis']['songs'][:1]['idx_segments_pitches'][0]:].tolist())
    song_info.append(h5_file['analysis']['segments_timbre'][h5_file['analysis']['songs'][:1]['idx_segments_timbre'][0]:].tolist())
    song_info.append([x.decode("utf-8") for x in h5_file['metadata']['similar_artists'][h5_file['metadata']['songs'][:1]['idx_similar_artists'][0]:].tolist()])
    song_info.append(float(h5_file['metadata']['songs'][:1]['artist_hotttnesss'][0]))
    song_info.append(str(h5_file['metadata']['songs'][:1]['song_id'][0].decode("utf-8")))
    song_info.append(float(h5_file['analysis']['songs'][:1]['start_of_fade_out'][0]))
    song_info.append(float(h5_file['analysis']['songs'][:1]['tempo'][0]))
    song_info.append(int(h5_file['analysis']['songs'][:1]['time_signature'][0]))
    song_info.append(int(h5_file['analysis']['songs'][:1]['time_signature_confidence'][0]))
    song_info.append(str(h5_file['metadata']['songs'][:1]['title'][0].decode("utf-8")))
    song_info.append(str(h5_file['analysis']['songs'][:1]['track_id'][0].decode("utf-8")))
    song_info.append(int(h5_file['musicbrainz']['songs'][:1]['year'][0]))
#         result.append(song_info)
#     return result
    return song_info
    
#     h5tocopy = open_h5_file_read(h5_file)
#     print(h5tocopy)
#     song_num = get_num_songs(h5tocopy)

 #   print(result)
#     h5tocopy.close()
#     return result


# In[40]:


def transform_s3(key, bucket='millionsongs605'):
    """
    REMEBER TO DO DEFENSIVE PROGRAMMING, WRAP IN TRY/CATCH
    """
    s3 = boto3.client('s3')
    
    with tempfile.NamedTemporaryFile(mode='wb') as tmp:
        s3.download_fileobj(bucket, key, tmp)
    
        try:
            with h5py.File(tmp.name) as h5:
               
                return process_h5_file(h5)
        except Exception:
            return []



# In[ ]:


processed = []
chunk_size = 5000


# data = pd.DataFrame()
letters = ["A","B","C","D","E","F","G","H","I","J","K","L","M","N","O","P","Q","R","S","T","U","V","W","X","Y","Z"]
for letter in letters:
    csv_name = "s3://millionsongprocessed/data_" + letter +".tsv"
    with smart_open.smart_open(csv_name, 'w') as fout:
        writer = csv.writer(fout, delimiter='\t', lineterminator='\n')
        print(letter)
        for prefix in get_prefixes(letter):

            if(transform_s3(prefix) ==[]):
                continue
            processed.append(transform_s3(prefix))

            if len(processed) % chunk_size == 0:
#                 part_data = rows_to_file(processed)
                print("running")

    #             data = pd.concat([data, part_data], axis=0)
                writer.writerows(processed)
                processed = []

#         part_data = rows_to_file(processed)
        print("still running")
#         data = pd.concat([data, part_data], axis=0)
        writer.writerows(processed)
        processed = []

#         data.to_csv(csv_name, sep='\t', index=False)
#         data = pd.DataFrame()

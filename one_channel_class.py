import requests
from googleapiclient.discovery import build
import pandas as pd
from typing import NoReturn
from dotenv import load_dotenv, find_dotenv
import os
from dateutil import parser
import isodate

load_dotenv(find_dotenv())
API_KEY = os.getenv('API_KEY')
DUMMY_URL_ONE = 'https://www.youtube.com/@ExtremeCode'
DUMMY_URL_TEST = 'https://www.youtube.com/@ExtremeCode'


class VideoDataOne:
    def __init__(self, api_key: str, dummy_url: str) -> NoReturn:
        self.api_key = api_key
        self.dummy_url = dummy_url
        self.channel_id = []
        self.youtube = build('youtube',
                             'v3',
                             developerKey=self.api_key)
        self.statistics_request = []
        self.playlist_id = ''
        self.video_ids = []
        self.data_row = []
        self.dummy_video_data = None
        self.video_data = None

    def get_url(self) -> str:
        sub_a = '?channel_id='
        sub_b = '","channelConversionUrl"'

        r = requests.get(self.dummy_url)
        id_a = r.text.find(sub_a, r.text.find(sub_a) + 1)
        id_b = r.text.find(sub_b)
        self.channel_id = r.text[id_a + len(sub_a): id_b]

        return self.channel_id

    def get_statistics(self) -> dict:
        r = self.youtube.channels().list(part='snippet,contentDetails,statistics',
                                         id=self.channel_id)

        self.statistics_request = r.execute()

        return self.statistics_request

    def get_ids(self) -> list:
        self.playlist_id = self.statistics_request['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        next_page_token = ''

        while next_page_token is not None:
            r = self.youtube.playlistItems().list(part="snippet,contentDetails",
                                                  maxResults=50,
                                                  playlistId=self.playlist_id,
                                                  pageToken=next_page_token)
            response = r.execute()

            for item in response['items']:
                self.video_ids.append(item['contentDetails']['videoId'])
            next_page_token = response.get('nextPageToken')
        return self.video_ids

    def get_data(self) -> pd.DataFrame:
        for id_index in range(0, len(self.video_ids), 50):
            r = self.youtube.videos().list(part="snippet,contentDetails,statistics",
                                           id=','.join(self.video_ids[id_index: id_index + 50]))

            response = r.execute()

            for cur_video in response['items']:
                user_row = {'video_id': cur_video['id']}

                columns = {'snippet': ['channelTitle', 'title', 'description', 'publishedAt', 'tags'],
                           'statistics': ['viewCount', 'likeCount', 'commentCount'],
                           'contentDetails': ['duration', 'caption', 'definition']}

                for key in columns.keys():
                    for value in columns[key]:
                        try:
                            user_row[value] = cur_video[key][value]
                        except BaseException:
                            user_row[value] = None

                self.data_row.append(user_row)

        self.dummy_video_data = pd.DataFrame(self.data_row)

        return self.dummy_video_data

    def data_preprocessing(self) -> pd.DataFrame:
        int_cols = ['viewCount', 'likeCount', 'commentCount']
        self.dummy_video_data[int_cols] = self.dummy_video_data[int_cols].astype('int')

        self.dummy_video_data['duration'] = self.dummy_video_data['duration'].apply(
            lambda x: isodate.parse_duration(x)).astype('timedelta64[m]')

        self.dummy_video_data['publishedAt_date'] = self.dummy_video_data['publishedAt'].apply(
            lambda x: parser.parse(x)).apply(pd.to_datetime)

        self.dummy_video_data['publishedAt_time'] = self.dummy_video_data['publishedAt'].apply(
            lambda x: parser.parse(x)).astype('str').apply(lambda x: x[11:19])

        self.dummy_video_data['publishedAt_day'] = self.dummy_video_data['publishedAt'].apply(
            lambda x: parser.parse(x).strftime('%A'))

        self.dummy_video_data['caption'] = self.dummy_video_data['caption'].astype('bool')
        self.dummy_video_data['title_length'] = self.dummy_video_data['title'].apply(lambda x: len(x))

        self.dummy_video_data['tags_count'] = self.dummy_video_data['tags'].apply(
            lambda x: len(x) if x is not None else 0)

        self.dummy_video_data.drop(columns=['publishedAt', 'tags'], inplace=True)

        self.dummy_video_data['publishedAt_time_hour'] = self.dummy_video_data['publishedAt_time'].apply(
            lambda x: (int(x[0:2]) * 3600 + int(x[3:5]) * 60 + int(x[6:])) / 3600)

        self.dummy_video_data['target'] = self.dummy_video_data['likeCount']
        self.dummy_video_data.drop(columns=['video_id', 'channelTitle', 'title', 'description', 'publishedAt_date',
                                            'publishedAt_time', 'likeCount'], inplace=True)

        self.dummy_video_data['caption'] = self.dummy_video_data['caption'].apply(lambda x: 1 if x == True else 0)
        self.dummy_video_data['definition'] = self.dummy_video_data['definition'].apply(lambda x: 1 if x == 'hd' else 0)
        self.dummy_video_data = pd.get_dummies(data=self.dummy_video_data, columns=['publishedAt_day'], prefix='day', drop_first=True)

        self.video_data = self.dummy_video_data
        return self.video_data


if __name__ == '__main__':
    video_data = VideoDataOne(API_KEY, DUMMY_URL_TEST) # for debug in ide
    video_data.get_url()
    video_data.get_statistics()
    video_data.get_ids()
    video_data.get_data()
    x = video_data.data_preprocessing()
    print(x)

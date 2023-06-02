import requests
from googleapiclient.discovery import build
import pandas as pd
from typing import NoReturn
from dotenv import load_dotenv, find_dotenv
import os

load_dotenv(find_dotenv())
API_KEY = os.getenv('API_KEY')
DUMMY_URL = ['https://www.youtube.com/@ExtremeCode', 'https://www.youtube.com/@Dzari',
             'https://www.youtube.com/@GlebMikhaylov', 'https://www.youtube.com/@Maksim-Verdikt',
             'https://www.youtube.com/@BicepsUaYoutube', 'https://www.youtube.com/@AlexLesleyforman',
             'https://www.youtube.com/@LofiGirl', 'https://www.youtube.com/@zCaxap']


class VideoDataSeveral:
    def __init__(self, api_key: str, dummy_url: list) -> NoReturn:
        self.api_key = api_key
        self.dummy_url = dummy_url
        self.channel_ids = []
        self.youtube = build('youtube',
                             'v3',
                             developerKey=self.api_key)
        self.statistics_request = []
        self.playlists_ids = []
        self.video_ids = []

    def get_url(self) -> list:
        sub_a = '?channel_id='
        sub_b = '","channelConversionUrl"'

        for fake_url in self.dummy_url:
            r = requests.get(fake_url)
            id_a = r.text.find(sub_a, r.text.find(sub_a) + 1)
            id_b = r.text.find(sub_b)
            self.channel_ids.append(r.text[id_a + len(sub_a): id_b])

        return self.channel_ids

    def get_statistics(self) -> list:
        for channel_id in self.channel_ids:
            r = self.youtube.channels().list(part='snippet,contentDetails,statistics',
                                             id=channel_id)
            self.statistics_request.append(r.execute())

        return self.statistics_request

    def get_playlists_id(self) -> list:
        for elem in self.statistics_request:
            self.playlists_ids.append(elem['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
        return self.playlists_ids

    def get_ids(self) -> list:
        for playlist_id in self.playlists_ids:
            next_page_token = ''

            while next_page_token != None:
                r = self.youtube.playlistItems().list(part="snippet,contentDetails",
                                                      maxResults=50,
                                                      playlistId=playlist_id,
                                                      pageToken=next_page_token)
                response = r.execute()

                for item in response['items']:
                    self.video_ids.append(item['contentDetails']['videoId'])
                next_page_token = response.get('nextPageToken')
        return self.video_ids

    def get_data(self) -> pd.DataFrame:
        data_row = []

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
                        except:
                            user_row[value] = None

                data_row += [user_row]

        return pd.DataFrame(data_row)


if __name__ == '__main__':
    video_data = VideoData(API_KEY, DUMMY_URL)  # for debug in ide
    video_data.get_url()
    video_data.get_statistics()
    video_data.get_playlists_id()
    video_data.get_ids()
    x = video_data.get_data
    print(x)

import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

import time
from datetime import datetime
import pandas as pd




# ************************************
# **** Get Lyrics From Text Files ****
# ************************************

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("GETTING LYRICS - ", current_time)

#text_link_df = pd.read_csv(r'links.txt', header=None, names=['Text_Link'])[0].to_list()
text_link_df = pd.read_csv(r'links.txt', header=None)[0].to_list()

lyrics = open("trippie_lyrics.txt", "a")

def get_lyrics(text_link):
    url = "https://ohhla.com/" + text_link
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    time.sleep(1)

    if soup.find('pre'):
        double_loc = soup.find('pre').text.find("\n\n") + 2
        print("PRE ",double_loc)
        lyrics.write(str(double_loc))
        return soup.find("pre").text[double_loc:]
    else:
        double_loc = html.find('\n\n') + 2
        print("ELSE", double_loc)
        lyrics.write(str(double_loc))
        return html[double_loc:]


lyrics_list = []

processes = []
with ThreadPoolExecutor(max_workers=10) as executor:
    for link in text_link_df:
        print(text_link_df.index(link))
        processes.append(executor.submit(get_lyrics, link))

    for task in as_completed(processes):
        lyrics_list.append(task.result())

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("FINISHED GETTING LYRICS - ", current_time)

with open('total_lyrics.txt', 'a') as f:
    for lyrics in set(lyrics_list):
        f.write(lyrics)
        f.write('\n' * 2)
    f.close()

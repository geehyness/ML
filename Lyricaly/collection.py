import requests
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor, as_completed

import time
from datetime import datetime
import pandas as pd

totalLinks = ["https://ohhla.com/all.html",
              "https://ohhla.com/all_two.html",
              "https://ohhla.com/all_three.html",
              "https://ohhla.com/all_four.html",
              "https://ohhla.com/all_five.html"]


def getParentLinks(parentLink, count):
    soup = BeautifulSoup(requests.get(parentLink).text, 'html.parser')
    grossLinks = [link['href'] for link in soup.findAll('a', href=True)]

    now = datetime.now()
    count += 1
    current_time = now.strftime("%H:%M:%S")
    print("Current Time =", current_time, "- Got Link #", count)
    print(" - Link:", grossLinks)

    # print("Got Link #",  count, time())
    return grossLinks[:-2]


# start=time()


linksPerTotalLinks = []

processes = []
with ThreadPoolExecutor(max_workers=10) as executor:
    count = 0
    for link in totalLinks:
        response = executor.submit(getParentLinks, link, count)
        processes.append(response)

    for task in as_completed(processes):
        linksPerTotalLinks.append(task.result())

# end = time()


allLinks = [item for sublist in linksPerTotalLinks for item in sublist]
uniqueLinks = []
uniqueLinks = [link for link in allLinks if link not in uniqueLinks]
linkDf = pd.DataFrame(uniqueLinks)

ohhla = linkDf[linkDf[0].apply(lambda x: x[:len("http://ohhla.com/")] == "http://ohhla.com")].index.tolist()
amazon = linkDf[linkDf[0].apply(lambda x: "www.amazon.com" in x)].index.tolist()
itunes = linkDf[
    linkDf[0].apply(lambda x: x[:len("https://itunes.apple.com")] == "https://itunes.apple.com")].index.tolist()
apk = linkDf[
    linkDf[0].apply(lambda x: x[:len("https://www.apkfollow.com")] == "https://www.apkfollow.com")].index.tolist()
allText = linkDf[linkDf[0].apply(lambda x: x[:len("all")] == "all")].index.tolist()
allHTML = linkDf[linkDf[0].apply(lambda x: x[:len(".html")] == ".html")].index.tolist()
rapReviews = linkDf[
    linkDf[0].apply(lambda x: x[:len("http://rapreviews.com/")] == "http://rapreviews.com")].index.tolist()

totalRemove = ohhla + amazon + itunes + apk + allText + allHTML + rapReviews

# linkDf2 = linkDf
# linkDf2.filter(allHTML)
# linkDf2.to_csv('unparented_links.txt', header=False, index=False)

linkDf.drop(totalRemove, inplace=True)
linkDf.to_csv('initial_directories.txt', header=False, index=False)



# ******************************
# **** Artist Sub Directory ****
# ******************************

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("GETTING ARTISTS' SUB DIRECTORY - ", current_time)

dir_list = pd.read_csv(r'initial_directories.txt', header=None)[0].to_list()

sub_dir_list = []


def get_sub_directories(parent_directory, index):
    url = "https://ohhla.com/" + parent_directory
    soup = BeautifulSoup(requests.get(url).text, 'html.parser')
    #soup = BeautifulSoup(requests.get(url).text, 'html.parser')
    gross_links = [parent_directory + link['href'] for link in soup.find_all('a', href=True) if "/" in link['href']
                   and "anonymous" not in link['href']]
    sub_now = datetime.now()
    sub_current_time = sub_now.strftime("%H:%M:%S")
    print(gross_links)
    return gross_links[:-2]


processes = []
with ThreadPoolExecutor(max_workers=50) as executor:
    for link in dir_list:
        processes.append(executor.submit(get_sub_directories, link, dir_list.index(link)+1))

print('PROCESSES - ', len(processes))

for task in as_completed(processes):
    print('LIST LENGTH - ', len(sub_dir_list))
    sub_dir_list.append(task.result())


unpacked_sub_dir_list = []
#[item for each_dir in sub_dir_list for item in each_dir]

for each_dir in sub_dir_list:
    for item in each_dir:
        print(item)
        unpacked_sub_dir_list.append(item)

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("FINISHED GETTING SUB DIRECTORIES - ", current_time)

pd.DataFrame(unpacked_sub_dir_list).to_csv('total_sub_directories.txt', header=False, index=False)



# **********************************
# **** Get Lyric TXT File Links ****
# **********************************

start_time = datetime.now()
current_time = start_time.strftime("%H:%M:%S")
print("GETTING TEXT LINKS - ", start_time)

all_dirs_list = pd.read_csv(r'album_directories.txt', header=None)[0].to_list()

text_links = []

txtLinks = open("text_links.txt", "a")


def get_text_links(parent_dir, count):
    url = "https://ohhla.com/" + parent_dir
    soup = BeautifulSoup(requests.get(url).text, 'html.parser')
    gross_links = [parent_dir + link['href'] for link in soup.find_all('a', href=True) if ".txt" in link['href']]

    sub_now = datetime.now()
    sub_current_time = sub_now.strftime("%H:%M:%S")

    for item in gross_links:
        txtLinks.write(item)
        txtLinks.write("\n")
        print(item)
    return gross_links[:-2]


processes = []
with ThreadPoolExecutor(max_workers=50) as executor:
    for link in all_dirs_list:
        processes.append(executor.submit(get_text_links, link, all_dirs_list.index(link)+1))

for task in as_completed(processes):
    text_links.append(task.result())


now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("SAVING TXT LINKS ", now, " - Duration: ", now - start_time)

pd.DataFrame(text_links).to_csv('text_links.txt', header=False, index=False)



unpacked_text_links = []
#[item for each_dir in text_links for item in each_dir]

for each_dir in text_links:
    for item in each_dir:
        print(item)

        unpacked_text_links.append(item)

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("FINISHED GETTING TEXT LINKS - ", current_time)

pd.DataFrame(unpacked_text_links).to_csv('total_text_links.txt', header=False, index=False)



# ************************************
# **** Get Lyrics From Text Files ****
# ************************************

now = datetime.now()
current_time = now.strftime("%H:%M:%S")
print("GETTING LYRICS - ", current_time)

text_link_df = pd.read_csv(r'total_text_links.txt', header=None, names=['Text_Link'])[0].to_list()


def get_lyrics(text_link):
    url = "https://ohhla.com/" + text_link
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'html.parser')
    time.sleep(1)

    if soup.find('pre'):
        double_loc = soup.find('pre').text.find("\n\n") + 2
        return soup.find("pre").text[double_loc:]
    else:
        double_loc = html.find('\n\n') + 2
        return html[double_loc:]


lyrics_list = []

processes = []
with ThreadPoolExecutor(max_workers=10) as executor:
    for count, link in enumerate(text_link_df['Text_Link']):
        print(count)
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

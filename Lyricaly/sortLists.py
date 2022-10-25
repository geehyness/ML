import pandas as pd


all_dirs_list = pd.read_csv(r'text_links.txt', header=None)[0].to_list()

links = open("links.txt", "a")

for item in all_dirs_list:
    if item.startswith("anonymous"):
        if item.startswith("anonymous//"):
            print("skip")
        else:
            links.write(item)
            links.write("\n")
            print(item)
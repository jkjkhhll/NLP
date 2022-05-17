from tqdm import tqdm
from mediawiki import MediaWiki
import pickle


wikipedia = MediaWiki()
cs = wikipedia.page("cognitive science")

all_links = set(cs.links)

for link in tqdm(cs.links):
    try:
        linked_page = wikipedia.page(link, preload=False)
    except:
        continue

    for nlink in linked_page.links:
        all_links.add(nlink)

with open("corpus_titles.pickle", "bw") as f:
    pickle.dump(all_links, f)

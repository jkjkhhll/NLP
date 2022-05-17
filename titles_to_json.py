import json
import pickle

with open("corpus_titles.pickle", "br") as in_f:
    with open("corpus_titles.json", "w") as out_f:
        titles = list(pickle.load(in_f))
        json.dump(titles, out_f)

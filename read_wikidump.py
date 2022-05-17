"""
Read and process Wikipedia XML dumps from .bz2 file.

Usage:
    read_wikidump grab [--fulltext] <dump_file.bz2> <titles_file.json> <out_file.json>
    read_wikidump redirects <dump_file.bz2> <titles_file.json> <out_file.json>
    read_wikidump (-h | --help | --version)

Commands:
    grab        Grab summaries or full texts of articles specified in <titles_file> 
    redirects   Find actual titles for redirected articles in <titles_file>

File format for <titles_file>:
    ['Title 1', 'Title 2', ... ]
    
Options:
    -h --help   Show this help screen.
    --fulltext  Grab full text (default is just the summary)
"""

import xml
import time
import json
from bz2 import BZ2File
from multiprocessing import Manager, Process, set_start_method, Value, Queue
import ctypes
import re

from tqdm import tqdm
import mwparserfromhell
import spacy
from docopt import docopt

from wikireader import WikiReader

args = docopt(__doc__, version="0.0.1")
nlp = spacy.load("en_core_web_sm")


def process_article(id, fulltext, aq, fq, corpus_titles):
    print(f"Worker {id} starting.")
    while True:
        if aq.empty():
            time.sleep(0.001)
            continue

        page_title, source, _ = aq.get()

        if page_title in corpus_titles:
            wc = mwparserfromhell.parse(source)

            if fulltext:
                # Grab entire article, excluding References etc.
                sections = wc.get_sections(
                    include_lead=True,
                    matches=r"^((?!References?|See also|External links).)*$",
                    include_headings=False,
                )
            else:
                # Grab summary and strip any Wikipedia tags
                sections = wc.get_sections(include_lead=True)[:1]

            text = ""

            # Remove files (images) and category tags (that would be cheating?)
            for section in sections:
                for node in section.nodes:
                    if "[[Category" in node or "[[File" in node:
                        wc.remove(node)
                text += section.strip_code()

            text = re.sub(r"<\/*ref>", "", text)
            text = re.sub(r"\n", "", text)

            doc = nlp(text)
            noun_lemmas = []

            for tok in doc:
                if tok.pos_ == "NOUN":
                    noun_lemmas.append(tok.lemma_)

            fq.put(json.dumps({"page": page_title, "text": " ".join(noun_lemmas)}))

        aq.task_done()


def process_redirect(id, aq, fq, corpus_titles):
    print(f"Worker {id} starting.")
    while True:
        if aq.empty():
            time.sleep(0.001)
            continue

        page_title, _, redirect = aq.get()

        if page_title in corpus_titles:
            if redirect != None:
                fq.put(json.dumps({"page": page_title, "actual_page": redirect}))
            else:
                fq.put(json.dumps({"page": page_title, "actual_page": page_title}))

        aq.task_done()


def writer(aq, fq, outfile: str, progmax: int, shutdown: Value, statuscode: Value):
    progress = tqdm(total=progmax)
    status_messages = {
        0: "Parsing articles...",
        1: "Parsing done. Waiting for writer...",
        2: "Writing done. Shutting down...",
    }

    with open(outfile, "w+") as out_file:

        while shutdown.value == False:
            if fq.empty():
                time.sleep(0.001)
                continue

            line = fq.get()
            out_file.write(line + "\n")

            progress.update(1)
            progress.set_description(
                f"{status_messages[statuscode.value]} aq={aq.qsize()} fq={fq.qsize()}"
            )

            fq.task_done()


def run_parser():
    set_start_method("spawn")

    wikifile = args["<dump_file.bz2>"]
    titlesfile = args["<titles_file.json>"]
    outfile = args["<out_file.json>"]

    with open(titlesfile, "r") as f:
        corpus_titles = json.load(f)

    n_workers = 8
    manager = Manager()

    # fq = file queue, for writing parsed documents to outfile
    fq = manager.Queue(maxsize=2000)
    # aq = article queue, for parsing and processing articles
    aq = manager.Queue(maxsize=2000)

    wiki = BZ2File(wikifile)
    reader = WikiReader(lambda ns: ns == 0, aq.put)

    workers = []
    for i in range(n_workers):
        if args["grab"]:
            workers.append(
                Process(
                    target=process_article,
                    args=(i, args["--fulltext"], aq, fq, corpus_titles),
                )
            )
        else:  # Parse redirects
            workers.append(
                Process(target=process_redirect, args=(i, aq, fq, corpus_titles))
            )
        workers[i].start()

    shutdown = Value(ctypes.c_bool, False)
    statuscode = Value(ctypes.c_int, 0)

    progmax = len(corpus_titles)  # Not all found, but a rough estimate of progress

    writer_process = Process(
        target=writer, args=(aq, fq, outfile, progmax, shutdown, statuscode)
    )
    writer_process.start()

    xml.sax.parse(wiki, reader)

    aq.join()
    statuscode.value = 1
    for w in workers:
        w.terminate()

    fq.join()

    statuscode.value = 2
    shutdown.value = True
    writer_process.join()

    print("All done.")


if __name__ == "__main__":
    run_parser()
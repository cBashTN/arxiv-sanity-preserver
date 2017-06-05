"""
Queries arxiv API and downloads papers (the query is a parameter).
The script is intended to enrich an existing database pickle (by default db.p),
so this file will be loaded first, and then new results will be added to it.
"""

import os
import time
import pickle
import random
import argparse
import urllib.request
import feedparser

from utils import Config, safe_pickle_dump

isInProductionMode = True
max_paper_index = 30000

def encode_feedparser_dict(d):
    """ 
    helper function to get rid of feedparser bs with a deep copy. 
    I hate when libs wrap simple things in their own classes.
    """
    if isinstance(d, feedparser.FeedParserDict) or isinstance(d, dict):
        j = {}
        for k in d.keys():
            j[k] = encode_feedparser_dict(d[k])
        return j
    elif isinstance(d, list):
        l = []
        for k in d:
            l.append(encode_feedparser_dict(k))
        return l
    else:
        return d


def parse_arxiv_url(url):
    """ 
    examples is http://arxiv.org/abs/1512.08756v2
    we want to extract the raw id and the version
    """
    ix = url.rfind('/')
    idversion = url[ix + 1:]  # extract just the id (and the version)
    parts = idversion.split('v')
    assert len(parts) == 2, 'error parsing url ' + url
    return parts[0], int(parts[1])



def load_database():
    # lets load the existing database to memory
    try:
        db = pickle.load(open(Config.db_path, 'rb'))
    except Exception as e:
        print('error loading existing database:')
        print(e)
        if isInProductionMode:
            print('Exit program')
            quit()
        else:
            print('Start new database: '+Config.db_path)
            db = {}
    return db


def store_database(data, path):
    print('Saving database with %d papers to %s' %
        (len(data), path))
    safe_pickle_dump(data, path)


def fetch_new_paper_info(args, base_url):
    # -----------------------------------------------------------------------------
    # main loop where we fetch the new results
    num_added_total = len(db)
    print('database has %d entries at start' % (num_added_total))
    
    for i in range(args.start_index, args.max_index, args.results_per_iteration):

        print("Results %i - %i" % (i, i + args.results_per_iteration))
        query = 'search_query=%s&sortBy=lastUpdatedDate&start=%i&max_results=%i' % (args.search_query,
                                                                                    i, args.results_per_iteration)
        with urllib.request.urlopen(base_url + query) as url:
            print(url)
            response = url.read()
        parse = feedparser.parse(response)
        num_added = 0
        num_skipped = 0
        for e in parse.entries:

            j = encode_feedparser_dict(e)

            # extract just the raw arxiv id and version for this paper
            rawid, version = parse_arxiv_url(j['id'])
            j['_rawid'] = rawid
            j['_version'] = version

            # add to our database if we didn't have it before, or if this is a
            # new version
            if not rawid in db or j['_version'] > db[rawid]['_version']:
                db[rawid] = j
                print('Updated %s added %s' %
                      (j['updated'].encode('utf-8'), j['title'].encode('utf-8')))
                num_added += 1
                num_added_total += 1
            else:
                num_skipped += 1

        # print some information
        print('Added %d papers, already had %d.' % (num_added, num_skipped))

        if len(parse.entries) == 0:
            print('Received no results from arxiv. Rate limiting? Exiting. Restart later maybe.')
            print(response)
            #break

        if num_added == 0 and args.break_on_no_added == 1:
            print('No new papers were added. Assuming no new papers exist. Exiting.')
            #break

        wait_a_couple_of_seconds(args.wait_time)
        
        return num_added

def wait_a_couple_of_seconds(wait_time):
    rand_wait_time = wait_time + random.uniform(0, 3)
    print('Sleeping for %i seconds' % (rand_wait_time, ))
    time.sleep(rand_wait_time)

if __name__ == "__main__":

    while True:

        # lets load the existing database to memory
        db = load_database()

        existing_files_count = len(db)
        print("Existing files count:"+str(existing_files_count))

        # parse input arguments
        parser = argparse.ArgumentParser()
        parser.add_argument('--search-query', type=str,
                            default='cat:cs.CV+OR+cat:cs.AI+OR+cat:cs.LG+OR+cat:cs.CL+OR+cat:cs.NE+OR+cat:stat.ML',
                            help='query used for arxiv API. See http://arxiv.org/help/api/user-manual#detailed_examples')
        parser.add_argument('--start-index', type=int, default=existing_files_count, help='0 = most recent API result')
        parser.add_argument('--max-index', type=int, default=max_paper_index, help='upper bound on paper index we will fetch')
        parser.add_argument('--results-per-iteration', type=int, default=200, help='passed to arxiv API')
        parser.add_argument('--wait-time', type=float, default=5.0, help='lets be gentle to arxiv API (in number of seconds)')
        parser.add_argument('--break-on-no-added', type=int, default=1, help='break out early if all returned query papers are already in db? 1=yes, 0=no')
        args = parser.parse_args()

        # misc hardcoded variables
        base_url = 'http://export.arxiv.org/api/query?'  # base api query url
        print('Searching arXiv for %s' % (args.search_query, ))

        # main loop where we fetch the new results
        num_added = fetch_new_paper_info(args, base_url)

        # save the database before we quit, if we found anything new
        if num_added > 0:
            store_database(db, Config.db_path)

        wait_a_couple_of_seconds(60)

        if isInProductionMode and len(db)>=max_paper_index:
            print('I have now ' + str(len(db)) +' paper info. let s stop this.')
            break
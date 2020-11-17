#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Library for semantic annotation of tabular data with the Wikidata knowledge graph"""

import pandas as pd
import requests
from collections import Counter
import re
import difflib
from datetime import date
from bs4 import BeautifulSoup
import time
import ftfy
import numpy as np
import random
import string
import os


def get_parallel(a, n):
    """Get input for GNU parallel based on a-list of filenames and n-chunks.
    The a-list is split into n-chunks. Offset and amount are provided."""
    k, m = divmod(len(a), n)
    # chunked = list((a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)))
    offset = ' '.join(list((str(i * k + min(i, m)) for i in range(n))))
    amount = ' '.join(list((str(k + min(i + 1, m) - min(i, m)) for i in range(n))))
    parallel = "parallel --delay 1 --linebuffer --link python3 bbw_cli.py "
    input_4_gnu_parallel = parallel + "--amount ::: " + amount + " ::: --offset  ::: " + offset
    return input_4_gnu_parallel


def random_user_agent(agent='bot4bbw-'):
    """Add random strings from the left and right sides to an input user agent."""
    letters = string.ascii_lowercase
    random_agent = '-' + ''.join(random.choice(letters) for i in range(random.randrange(4, 9)))
    return random.choice(letters) + agent + random_agent


def get_SPARQL_dataframe(name, url="https://query.wikidata.org/sparql", extra=''):
    """
    Parameters
    ----------
    name : str
        Possible mention in wikidata.
    url : str, optional
        SPARQL-endpoint. The default is "https://query.wikidata.org/sparql".
    extra : str
        An extra parameter that will be also SELECTed in the SPARQL query.
    Returns
    -------
    output : pd.DataFrame
        Dataframe created from the json-file returned by SPARQL-endpoint.
    """
    name = name.replace('"', '\\\"')
    if extra:
        subquery = """
        ?item rdfs:label ?itemLabel.
        FILTER (lang(?itemLabel) = "en")."""
    else:
        subquery = ""
    query = "SELECT DISTINCT ?item " + extra + """?itemType ?p1 ?p2 ?value ?valueType ?valueLabel ?psvalueLabel WHERE {
                ?item ?p1 """ + '"' + name + '"' + """@en;
                ?p2 ?value.""" + subquery + """
                OPTIONAL { ?item wdt:P31 ?itemType. }
                OPTIONAL { ?value wdt:P31 ?valueType. }
                OPTIONAL {
                    ?wdproperty wikibase:claim ?p2 ;
                        wikibase:statementProperty ?psproperty .
                    ?value ?psproperty ?psvalue .
                }
                SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
            }
            LIMIT 100000
            """
    try:
        r = requests.get(url,
                         params={'format': 'json', 'query': query},
                         headers={'User-Agent': random_user_agent()},
                         timeout=12.5)
        if r.status_code == 429:
            time.sleep(int(r.headers["Retry-After"]))
            r = requests.get(url,
                             params={'format': 'json', 'query': query},
                             headers={'User-Agent': random_user_agent()},
                             timeout=12.5)
        results = r.json().get('results').get("bindings")
        for prop in results:
            if 'psvalueLabel' in prop and prop.get('psvalueLabel').get('value') is not None:
                prop['valueLabel']['value'] = prop.get('psvalueLabel').get('value')
            prop.update((key, value.get('value')) for key, value in prop.items())
        if len(results) != 0:
            output = pd.DataFrame(results, dtype=str)
        else:
            output = None
    except Exception:
        output = None

    return output


def get_SPARQL_dataframe_item(name, url="https://query.wikidata.org/sparql"):
    """
    Parameters
    ----------
    name : str
        Possible item in wikidata.
    url : str, optional
        SPARQL-endpoint. The default is "https://query.wikidata.org/sparql".
    Returns
    -------
    output : pd.DataFrame
        Dataframe created from the json-file returned by SPARQL-endpoint.
    """
    name = name.replace('"', '\\\"')
    query = """SELECT REDUCED ?value ?valueType ?p2 ?item ?itemType ?itemLabel WHERE {
                ?value rdfs:label """ + '"' + name + '"' + """@en;
                wdt:P31 ?valueType.
                ?item ?p2 [ ?x """ + '"' + name + '"' + """@en].
                ?item wdt:P31 ?itemType.
                ?item rdfs:label ?itemLabel.
                FILTER((LANG(?itemLabel)) = "en").
            }
            LIMIT 10000
            """
    try:
        r = requests.get(url,
                         params={'format': 'json', 'query': query},
                         headers={'User-Agent': random_user_agent()},
                         timeout=2.5)
        if r.status_code == 429:
            time.sleep(int(r.headers["Retry-After"]))
            r = requests.get(url,
                             params={'format': 'json', 'query': query},
                             headers={'User-Agent': random_user_agent()},
                             timeout=2.5)
        results = r.json().get('results').get('bindings')
        for prop in results:
            prop.update((key, value.get('value')) for key, value in prop.items())
        if len(results) > 0:
            output = pd.DataFrame(results, dtype=str)
        else:
            output = None
    except Exception:
        output = None

    return output


def get_SPARQL_dataframe_prop(prop, value, url="https://query.wikidata.org/sparql"):
    value = [val.replace('"', '\\\"') for val in value]
    subquery = []
    subquery.extend([""" wdt:""" + str(prop) + """ [ ?p """ + '"' + str(value) + '"' + """@en ] ;
        wdt:""" + str(prop) + " ?value" + str(ind) + ";" for ind, (prop, value) in enumerate(zip(prop, value))])
    subquery = ' '.join(subquery)
    # wdt:"""+ str(prop) + """ [ ?p """ + '"' + str(value) + '"' + """@en ] ;
    #    wdt:"""+ str(prop) + """ ?value0;
    query = """
    SELECT REDUCED ?item ?itemType ?itemLabel ?p2 ?value ?valueType ?valueLabel ?psvalueLabel WHERE {
  ?item """ + subquery + """
        ?p2 ?value.
  ?item wdt:P31 ?itemType;
        rdfs:label ?itemLabel.
  FILTER (lang(?itemLabel) = "en").
  OPTIONAL {
  ?value wdt:P31 ?valueType .}
  OPTIONAL {?wdproperty wikibase:claim ?p2 ;
                        wikibase:statementProperty ?psproperty .
            ?value ?psproperty ?psvalue .}
   SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
   }
    LIMIT 50000
    """
    try:
        r = requests.get(url,
                         params={'format': 'json', 'query': query},
                         headers={'User-Agent': random_user_agent()},
                         timeout=5)  # To avoid 1 min. timeouts.
        if r.status_code == 429:
            time.sleep(int(r.headers["Retry-After"]))
            r = requests.get(url,
                             params={'format': 'json', 'query': query},
                             headers={'User-Agent': random_user_agent()},
                             timeout=5)
        results = r.json().get('results').get('bindings')
        for prop in results:
            if 'psvalueLabel' in prop and prop.get('psvalueLabel').get('value') is not None:
                prop['valueLabel']['value'] = prop.get('psvalueLabel').get('value')
            prop.update((key, value.get('value')) for key, value in prop.items())
        if len(results) > 0:
            output = pd.DataFrame(results, dtype=str)
        else:
            output = None
    except Exception:
        output = None

    return output


def get_SPARQL_dataframe_type(name, datatype, url="https://query.wikidata.org/sparql"):
    name = name.replace('"', '\\\"')
    query = """SELECT REDUCED ?item ?itemLabel WHERE {
        {?item  rdfs:label """ + '"' + name + '"' + """@en.} UNION
        {?item  skos:altLabel """ + '"' + name + '"' + """@en.}
        ?item wdt:P31 wd:""" + datatype + """.
        SERVICE wikibase:label { bd:serviceParam wikibase:language "en". }
        }
        LIMIT 10000"""
    try:
        r = requests.get(url,
                         params={'format': 'json', 'query': query},
                         headers={'User-Agent': random_user_agent()},
                         timeout=2)
        if r.status_code == 429:
            time.sleep(int(r.headers["Retry-After"]))
            r = requests.get(url,
                             params={'format': 'json', 'query': query},
                             headers={'User-Agent': random_user_agent()},
                             timeout=2)
        results = r.json().get('results').get('bindings')
        for prop in results:
            prop.update((key, value.get('value')) for key, value in prop.items())
        if len(results) > 0:
            output = pd.DataFrame(results, dtype=str)
        else:
            output = None
    except Exception:
        output = None

    return output


def get_SPARQL_dataframe_type2(datatype, url="https://query.wikidata.org/sparql"):
    query = """SELECT REDUCED ?itemLabel WHERE {
        ?item wdt:P31 wd:""" + datatype + """;
              rdfs:label ?itemLabel.
        FILTER (lang(?itemLabel) = "en").
        }
        LIMIT 1000000"""
    try:
        r = requests.get(url,
                         params={'format': 'json', 'query': query},
                         headers={'User-Agent': random_user_agent()},
                         timeout=59)
        if r.status_code == 429:
            time.sleep(int(r.headers["Retry-After"]))
            r = requests.get(url,
                             params={'format': 'json', 'query': query},
                             headers={'User-Agent': random_user_agent()},
                             timeout=59)
        results = r.json().get('results').get('bindings')
        for prop in results:
            prop.update((key, value.get('value')) for key, value in prop.items())
        if len(results) > 0:
            output = pd.DataFrame(results, dtype=str)
        else:
            output = None
    except Exception:
        output = None

    return output


def get_openrefine_bestname(name):
    """
    Parameters
    ----------
    name : str
        Possible entity label in wikidata.
    Returns
    -------
    bestname : str
        The best suggestion returned by OpenRefine-Reconciliation API-service.
    """
    # Alternative url: "https://openrefine-wikidata.toolforge.org/en/api"
    url = "https://wikidata.reconci.link/en/api"
    params = {"query": name}

    try:
        r = requests.get(url=url, params=params, headers={'User-Agent': random_user_agent()}, timeout=1)
        results = r.json().get('result')
        bestname = results[0].get('name')
    except Exception:
        bestname = None
    return bestname


def get_wikidata_URL(name):
    """
    Parameters
    ----------
    name : str
        Possible entity label in wikidata.
    Returns
    -------
    bestname : str
        The best suggestion returned by Wikidata API-service.
    """

    url = "https://www.wikidata.org/w/api.php"
    params = {"action": "query",
              "srlimit": "1",
              "format": "json",
              "list": "search",
              "srqiprofile": "wsum_inclinks_pv",
              "srsearch": name}

    try:
        r = requests.get(url=url, params=params, headers={'User-Agent': random_user_agent()}, timeout=1)
        results = r.json()
        if len(results) != 0:
            query = results.get('query')
            if query:
                search = query.get('search')
                if len(search) > 0:
                    bestname = search[0].get("title")
                    if bestname:
                        URL = 'http://www.wikidata.org/entity/' + bestname
        if not URL:
            URL = None
    except Exception:
        URL = None
    return URL


def get_wikidata_title(url):
    """
    Parameters
    ----------
    url : str
        URL of a Wikidata page.
    Returns
    -------
    title: str
        Title of a Wikidata page.
    """
    try:
        url = url.replace('http://www.wikidata.org/prop/direct/', 'http://www.wikidata.org/entity/')
        params = {"action": "wbgetentities",
                  "format": "json",
                  "props": "labels",
                  "ids": url.split('/')[-1]}
        r = requests.get(url, params=params, headers={'User-Agent': random_user_agent()}, timeout=5).json()
        title = r.get('entities').get(url.split('/')[-1]).get('labels').get('en').get('value')
    except Exception:
        title = ''
    return title


def get_title(url):
    """
    Parameters
    ----------
    url : str
        URL of a web-page.
    Returns
    -------
    title: str
        Title of a web-page.
    """
    try:
        r = requests.get(url, headers={'User-Agent': random_user_agent()}, timeout=1)
        title = BeautifulSoup(r.text, features="lxml").title.text
        title = title.replace(' - Wikidata', '')
    except Exception:
        title = None
    return title


def get_wikimedia2wikidata_title(wikimedia_url):
    """
    Parameters
    ----------
    wikimedia_url : str
        URL of a Wikimedia Commons page.
    Returns
    -------
    title : str
        The title of the corresponding Wikidata page.
    """
    try:
        r = requests.get(wikimedia_url, headers={'User-Agent': random_user_agent()}, timeout=1)
        soup = BeautifulSoup(r.content, 'html.parser')
        redirect_url = soup.find(class_="category-redirect-header")
        if redirect_url:
            redirect_url = redirect_url.find("a").get("href")
            r = requests.get("https://commons.wikimedia.org" + redirect_url,
                             headers={'User-Agent': random_user_agent()}, timeout=1)
            soup = BeautifulSoup(r.content, 'html.parser')
        wikidata_url = soup.find('a', title="Edit infobox data on Wikidata").get('href')
        # time.sleep(0.25)
        title = get_title(wikidata_url)
    except Exception:
        title = None
    return title


def get_wikipedia2wikidata_title(wikipedia_title):
    """
    Parameters
    ----------
    wikipedia_title : str
        Title of a Wikipedia article.
    Returns
    -------
    bestname : str
        The title of the corresponding entity in Wikidata.
    """
    url = "https://en.wikipedia.org/w/api.php"
    params = {"action": "query",
              "prop": "pageprops",
              "ppprop": "wikibase_item",
              "redirects": "1",
              "titles": wikipedia_title,
              "format": "json"}

    try:
        r = requests.get(url=url, params=params, headers={'User-Agent': random_user_agent()}, timeout=1)
        pages = r.json().get('query').get('pages')
        if pages.get('-1'):
            bestname = None
        else:
            # bestname = [k for k in pages.values()][0].get('title')
            wikidataID = [k for k in pages.values()][0].get('pageprops').get('wikibase_item')
            bestname = get_title("https://www.wikidata.org/wiki/" + wikidataID).replace(' - Wikidata', '')
    except Exception:
        bestname = None
    return bestname


def get_searx_bestname(name):
    """
    Parameters
    ----------
    name : str
        Possible entity label in wikidata.
    Returns
    -------
    bestname : str
        A few best suggestions returned by the Searx metasearch engine.
    """
    name_cleaned = name.replace('!', ' ').replace('#', ' ').replace(':-', ' -')
    url = os.getenv("BBW_SEARX_URL", "http://localhost:80")
    engines = "!yh !ddd !eto !bi !ew !et !wb !wq !ws !wt !wv !wy !tl !qw !mjk !nvr !wp !cc !wd !ddg !sp !yn !dc "
    data = {"q": engines + name_cleaned, "format": "json"}
    try:
        results = requests.get(url, data=data, headers={'User-Agent': random_user_agent()}).json()
        if 'results' not in locals():
            raise Exception
        bestname = []
        medianame = []
        # Process infoboxes
        if len(results.get('infoboxes')) > 0:
            bestname.extend([x.get('infobox') for x in results.get('infoboxes')])
        # Process suggestions
        if len(results.get('suggestions')) > 0:
            bestname.extend([k for k in results.get('suggestions') if not re.search("[\uac00-\ud7a3]", k)])
            for sugg in results.get('suggestions'):
                splitsugg = sugg.split()
                if len(splitsugg) > 2 and not re.search("[\uac00-\ud7a3]", sugg):
                    bestname.extend([' '.join(splitsugg[:-1])])
            best_sugg = difflib.get_close_matches(name, [k for k in results.get('suggestions') if
                                                         not re.search("[\uac00-\ud7a3]", k)], n=1, cutoff=0.65)
            try:
                data2 = {"q": engines + best_sugg[0], "format": "json"}
                results2 = requests.get(url, data=data2, headers={'User-Agent': random_user_agent()}).json()
                if results2:
                    if len(results2.get('infoboxes')) > 0:
                        bestname.extend([x.get('infobox') for x in results2.get('infoboxes')])
            except Exception:
                pass
        # Process corrections
        if len(results.get('corrections')) > 0:
            corrections = [corr for corr in results.get('corrections') if '"' not in corr]
            if len(corrections) > 0:
                for correction in corrections:
                    try:
                        data3 = {"q": engines + correction, "format": "json"}
                        results3 = requests.get(url, data=data3, headers={'User-Agent': random_user_agent()}).json()
                        if results3:
                            if len(results3.get('infoboxes')) > 0:
                                bestname.extend([x.get('infobox') for x in results3.get('infoboxes')])
                    except Exception:
                        pass
                bestname.extend(corrections)
        # Process search results
        if len(results.get('results')) > 0:
            for i, result in enumerate(results.get('results')):
                url = result.get('url')
                parsed_url=result.get('parsed_url')
                if len(parsed_url) > 1:
                    hostname = parsed_url[1]
                raw_title = result.get('title')
                if i == 1:
                    bestname.append(
                        raw_title.split(' | ')[0].split(" - ")[0].split(" ? ")[0].split(" ? ")[0].split(' \u2014 ')[
                            0].split(' \u2013 ')[0].replace('Talk:', '').replace('Category:', '').replace('...', ''))
                if ("wiki" in url) and not raw_title.endswith('...'):
                    bestname.append(
                        raw_title.split(" - ")[0].split(" ? ")[0].split(" ? ")[0].split(' \u2014 ')[0].split(
                            ' \u2013 ')[0].replace('Talk:', '').replace('Category:', ''))
                if ("wiki" in url) and raw_title.endswith('...') and ("Wikidata:SPARQL" not in result.get('title')):
                    title = get_title(url)
                    if title:
                        bestname.append(
                            title.split(" - ")[0].split(" ? ")[0].split(" ? ")[0].split(' \u2014 ')[0].split(
                                ' \u2013 ')[0].replace('Talk:', '').replace('Category:', ''))
                if hostname:
                    if hostname.endswith('.wikimedia.org'):
                        title = get_wikimedia2wikidata_title(url)
                        if title:
                            medianame.append(
                                title.split(" - ")[0].split(" ? ")[0].split(" ? ")[0].split(' \u2014 ')[0].split(
                                    ' \u2013 ')[0])
                if "dict" in url:
                    bestname.append(raw_title.split(' : ')[0].split(' | ')[0])
                raw_match = difflib.get_close_matches(name, [
                    raw_title.replace(' ...', '').replace(' …', '').split(' | ')[0].split(" - ")[0].split(' \u2014 ')[
                        0].split(' \u2013 ')[0]], n=1, cutoff=0.7)
                if len(raw_match) == 1:
                    bestname.append(raw_match[0])
        if len(bestname) > 0:
            bestname = [best for best in bestname if best != name]
        suggestions = list(set(difflib.get_close_matches(name, bestname, n=3, cutoff=0.41)))
        suggestions = suggestions + [get_openrefine_bestname(best) for best in suggestions]
        suggestions = suggestions + [get_wikipedia2wikidata_title(best) for best in suggestions]
        suggestions = list(set([best for best in suggestions if best]))
        bestname = difflib.get_close_matches(name, suggestions, n=3, cutoff=0.7)
        if 'results' in locals():
            if len(results.get('infoboxes')) > 0:
                bestname.extend([x.get('infobox') for x in results.get('infoboxes')])
        if 'results2' in locals():
            if len(results2.get('infoboxes')) > 0:
                bestname.extend([x.get('infobox') for x in results2.get('infoboxes')])
        if 'results3' in locals():
            if len(results3.get('infoboxes')) > 0:
                bestname.extend([x.get('infobox') for x in results3.get('infoboxes')])
        if len(bestname) == 0:
            bestname = difflib.get_close_matches(name, suggestions, n=3, cutoff=0.41)
            if len(bestname) == 0:
                bestname = None
        if len(medianame) > 0:
            bestname.extend(medianame)
        if len(bestname) > 0:
            bestname = list(set([best for best in bestname if best != name]))
            if len(bestname) == 0:
                bestname = None
    except Exception:
        bestname = None
    return bestname


def isfloat(value):
    try:
        float(value.replace(',', ''))
        return True
    except ValueError:
        return False


def get_common_class(classes, url="https://query.wikidata.org/sparql"):
    """
    Parameters
    ----------
    classes : list
        List of Wikidata entities.
    url : str, optional
        SPARQL-endpoint. The default is "https://query.wikidata.org/sparql".
    Returns
    -------
    output : str
        The common Wikidata class for a list of Wikidata entities.
    """
    if not isinstance(classes, list):
        print("Error:", classes, "is not a list of classes. ")
        return
    classes = [entity.replace('http://www.wikidata.org/entity/', '') for entity in classes]
    lengths = ['?len' + entity for entity in classes]
    length = '(' + ' + '.join(lengths) + ' as ?length)'
    subquery = []
    for entity, Qlength in zip(classes, lengths):
        subquery.append("""
    SERVICE gas:service {
        gas:program gas:gasClass "com.bigdata.rdf.graph.analytics.SSSP" ;
                    gas:in wd:""" + entity + """ ;
                    gas:traversalDirection "Forward" ;
                    gas:out ?super ;
                    gas:out1 """ + Qlength + """ ;
                    gas:maxIterations 10 ;
                    gas:linkType wdt:P279 .
      }
    """)
    subquery = ' '.join(subquery)
    query = """PREFIX gas: <http://www.bigdata.com/rdf/gas#>
    SELECT ?super """ + length + """ WHERE {""" + subquery + """
    } ORDER BY ?length
    LIMIT 1"""
    try:
        r = requests.get(url,
                         params={'format': 'json', 'query': query},
                         headers={'User-Agent': random_user_agent()})
        results = r.json().get('results').get('bindings')
        output = results[0].get('super').get('value')
    except Exception:
        output = classes[0]

    return output


def get_one_class(classes):
    """
    Takes a list of two tuples with a class and the number of times it has appeared in a column.
    Returns a common class.
    """
    if not classes:
        return None
    if len(classes) == 1 or (len(classes) == 2 and classes[0][1] > classes[1][1]):
        return classes[0][0]
    if len(classes) == 2 and classes[0][1] == classes[1][1]:
        one_class = get_common_class([classes[0][0], classes[1][0]])
        if one_class == "http://www.wikidata.org/entity/Q35120":
            return classes[0][0]
        else:
            return one_class
    if len(classes) > 2:
        print('ERROR: More than two classes in get_one_class().')
        return None


def lookup(name_in_data, metalookup=True, openrefine=False):
    """
    Parameters
    ----------
    name_in_data : str
        Search string.
    Returns
    -------
    list = [WDdf, how_matched]
        WDdf is a dataframe with SPARQL-request
        how_matched shows how the string was matched
            0: SPARQL-Wikidata
            1: OpenRefine Suggest API
            2: Searx-metasearch
    """
    how_matched = ''
    proper_name = ''
    # Search entity using WD SPARQL-endpoint
    WDdf = get_SPARQL_dataframe(name_in_data)
    if isinstance(WDdf, pd.DataFrame):
        proper_name = name_in_data
        how_matched = 'SPARQL'  # This means we have found a mention of 'name_in_data' in Wikidata using single SPARQL-query
    if isinstance(WDdf, pd.DataFrame):
        if 'item' in WDdf.columns:
            if all(WDdf.item.str.contains('wikipedia')):
                WDdf = None
    # Searx-metasearch-engine API
    if metalookup:
        if not isinstance(WDdf, pd.DataFrame):
            proper_name = get_searx_bestname(name_in_data)
            if proper_name:
                test_list = []
                for proper in proper_name:
                    e = get_SPARQL_dataframe(proper)
                    if isinstance(e, pd.DataFrame):
                        test_list.append(e)
                if len(test_list) > 0:
                    WDdf = pd.concat(test_list)
                    how_matched = 'SearX'  # proper_name is found in Wikidata
    # OpenRefine-Reconciliation API
    if openrefine:
        if not isinstance(WDdf, pd.DataFrame):
            proper_name = get_openrefine_bestname(name_in_data)
            if proper_name:
                WDdf = get_SPARQL_dataframe(proper_name)
                how_matched = 'OpenRefine'  # proper_name is found in Wikidata
    return [WDdf, how_matched, proper_name]


def detect_name(value):
    """
    This is an extended function from https://github.com/IBCNServices/CSV2KG/blob/master/csv2kg/util.py
    It detects names like 'V. Futter' or 'Ellen V. Futter' and returns 'Futter' or 'Ellen Futter'
    """
    match2 = re.match("^(\w\. )+([\w\-']+)$", value, re.UNICODE)
    match3 = re.match("^([\w\-']+ )+(\w\. )+([\w\-']+)$", value, re.UNICODE)
    if match2 is not None:
        return match2.group(2)
    if match3 is not None:
        return match3.group(1) + match3.group(3)
    return None


def match(WDdf, target_value):
    """Performs contextual matching for input dataframe and input target_value.
    Returns the dataframe constrained to the objects equal to target_value."""
    if isfloat(target_value):
        target_value = target_value.replace(',', '')
    isdate = re.match(r"^\d{4}-\d{2}-\d{2}", target_value)
    # 0. Normalize dates
    match_date = re.match(r"^(\d{4})/(\d{2})/(\d{2})$", target_value)
    if match_date:
        isdate = True
        target_value = match_date[1] + "-" + match_date[2] + "-" + match_date[3]
    # 1. exact matching of valueLabels
    df = WDdf[WDdf.valueLabel == target_value]
    # 2a. case-insensitive exact matching of valueLabels
    if df.empty and not isfloat(target_value):
        df = WDdf[WDdf.valueLabel.str.lower() == str.lower(target_value)]
        # 2b. inexact matching of valueLabels with high cuttoff=0.95
        if df.empty:
            approx_matches = difflib.get_close_matches(target_value, WDdf.valueLabel.to_list(), n=3, cutoff=0.95)
            if len(approx_matches) == 0:
                approx_matches = difflib.get_close_matches(target_value, WDdf.valueLabel.to_list(), n=3, cutoff=0.5)
            if len(approx_matches) > 0:
                df = WDdf[WDdf.valueLabel.isin(approx_matches)]
            else:
                if detect_name(target_value):
                    df = WDdf[WDdf.valueLabel.apply(
                        lambda x: all(word in x.lower() for word in detect_name(target_value).lower()))]
    # 3. approximate date matching
    if df.empty and isdate:
        wd_dates = [x for x in WDdf.valueLabel.to_list() if re.match(r"^\d{4}-\d{2}-\d{2}", x)]
        target_datetime = date.fromisoformat(target_value)
        approximate_match = min(wd_dates, key=lambda x: abs(date.fromisoformat(x[:10]) - target_datetime))
        # check that approximate date is within 6 months
        delta = date.fromisoformat(approximate_match[:10]) - target_datetime
        if abs(delta.days) < 183:
            df = WDdf[WDdf.valueLabel == approximate_match]
    # 4. approximate floating numbers matching
    if df.empty and isfloat(target_value):
        wd_floats = [x for x in WDdf.valueLabel.to_list() if isfloat(x)]
        # get only one approximate match
        # approximate_match = min(wd_floats, key=lambda x: abs(float(x) - float(target_value)))
        # get all approximate matches within a 2% range
        all_approximate_matches = [x for x in wd_floats if
                                   (abs(float(x) - float(target_value)) <= 0.02 * abs(float(x)))]
        if len(all_approximate_matches) > 0:
            df = WDdf[WDdf.valueLabel.isin(all_approximate_matches)]

    return df


def preprocessing(filecsv):
    """Simple preprocessing of a dataframe using ftfy.fix_text()."""
    filecsv = filecsv.fillna("")
    if len(filecsv.columns) == 1:  # Data augmentation for single-column tables
        filecsv[1] = filecsv[0]
    filecsv = filecsv.applymap(lambda x: ftfy.fix_text(x))  # fix encoding and clean text
    return filecsv


def contextual_matching(filecsv, filename='', default_cpa=None, default_cea=None, default_nomatch=None,
                        step3=False, step4=False, step5=True, step6=True):
    """Five-steps contextual matching for an input dataframe filecsv.
    Step 2 is always executed. Steps 3-6 are optional.
    The lists cpa_list and cea_list with annotations are returned."""
    if default_cpa:
        cpa_list = default_cpa
    else:
        cpa_list = []
    if default_cea:
        cea_list = default_cea
    else:
        cea_list = []
    if default_nomatch:
        nomatch = default_nomatch
    else:
        nomatch = []
    (rows, cols) = filecsv.shape
    nomatch_row = []
    fullymatched_rows = []
    cpa_ind = len(cpa_list)
    cea_ind = len(cea_list)
    # STEP 2 in the workflow
    step2 = True  # Step 2 is always executed
    if step2:
        for row in range(1, rows):  # We start here from row=1, because there are "col0" and "col1" in row=0
            name_in_data = filecsv.iloc[row, 0]
            [WDdf, how_matched, proper_name] = lookup(name_in_data)  # Lookup using the value from the 0-column
            this_row_item = []
            matches_per_row = 0
            cpa_row_ind = len(cpa_list)
            # for each other column look for a match of the value within the wikidata dataframe
            if isinstance(WDdf, pd.DataFrame):
                if not WDdf.empty:
                    for col in range(1, cols):
                        try:
                            df = match(WDdf, filecsv.iloc[row, col])
                            df_prop = df[(df.p2.str.contains('http://www.wikidata.org/')) & (
                                ~df.item.str.contains('/statement/'))]
                            properties = [
                                x.replace("/prop/P", "/prop/direct/P").replace("/direct-normalized/", "/direct/") for x
                                in df_prop.p2.to_list()]
                            properties = list(set(zip(properties, df_prop.item.to_list())))
                            if len(properties) > 0:
                                matches_per_row += 1
                                if matches_per_row == cols - 1:
                                    fullymatched_rows.append(row)
                            item = list(set(df_prop.item.to_list()))
                            if 'itemType' in df_prop:
                                itemType = list(set([k for k in df_prop.itemType.to_list() if k is not np.nan]))
                            else:
                                itemType = []
                            df_value = df[
                                (~df.value.str.contains('/statement/')) & (df.value.str.contains('wikidata.org'))]
                            if not df_value.empty:
                                value, valueType = list(set(df_value.value.to_list())), list(
                                    set([k for k in df_value.valueType.to_list() if k is not np.nan]))
                            else:
                                value, valueType = [], []
                            if properties and item:
                                cpa_list.append(
                                    [filename, row, 0, col, properties, item, itemType, how_matched, proper_name])
                            if item:
                                cea_list.append([filename, row, 0, item, itemType, how_matched, proper_name])
                                this_row_item.extend(item)
                            if value:
                                cea_list.append(
                                    [filename, row, col, value, valueType, 'Step 2: ' + how_matched, proper_name])
                        except Exception:
                            pass
            else:
                nomatch.append([filename, row, name_in_data, proper_name])
            # Take the most possible item for this row and remove the properties which are not taken from this item
            if len(this_row_item) > 0:
                this_row_item = Counter(this_row_item).most_common(1)[0][0]
                for i, cpa_row in enumerate(cpa_list[cpa_row_ind:]):
                    if len(cpa_row[4]) > 0:
                        cpa_list[cpa_row_ind + i][4] = [prop for prop in cpa_row[4] if prop[1] == this_row_item]
            # Define the unannotated rows
            if row == rows - 1:  # After the last row
                nomatch_row = [r for r in range(1, rows) if r not in fullymatched_rows]
    # Choose only entity columns, not the literal columns
    entity_columns = list(set([k[2] for k in cea_list[cea_ind:] if k[2] != 0 and k[3]]))

    # STEP 3 in the workflow
    if step3:
        # MATCHING item,itemType,value and valueType via properties and values in the entity-columns
        # Calculate the properties and find the item, itemType, value and valueType:
        col_prop = {}
        for row_prop in cpa_list[cpa_ind:]:
            if len(row_prop[4]) > 0:
                if col_prop.get(row_prop[3]):
                    col_prop[row_prop[3]].extend([cprop[0].split('/')[-1] for cprop in row_prop[4]])
                else:
                    col_prop[row_prop[3]] = [cprop[0].split('/')[-1] for cprop in row_prop[4]]
        col_prop.update((key, Counter(value).most_common(1)[0][0]) for key, value in col_prop.items())
        if len(entity_columns) > 0:
            for nrow in nomatch_row or []:
                try:  # Try to use ALL entity columns AT ONCE and their property-relations to the main column
                    WDdf = get_SPARQL_dataframe_prop(prop=[col_prop[ncol] for ncol in entity_columns],
                                                     value=[filecsv.iloc[nrow, ncol] for ncol in entity_columns])
                    bestname = list(set(
                        difflib.get_close_matches(filecsv.iloc[nrow, 0], WDdf.itemLabel.to_list(), n=3, cutoff=0.81)))
                    WD = WDdf[WDdf.itemLabel.isin(bestname)]
                    for col in range(1, cols):
                        try:
                            df = match(WD, filecsv.iloc[nrow, col])
                            item = list(set(df.item.to_list()))
                            if 'itemType' in df.columns:
                                itemType = list(set([k for k in df.itemType.to_list() if k is not np.nan]))
                            else:
                                itemType = []
                            df_value = df[
                                (~df.value.str.contains('/statement/')) & (df.value.str.contains('wikidata.org'))]
                            if not df_value.empty:
                                value, valueType = list(set(df_value.value.to_list())), list(
                                    set([k for k in df_value.valueType.to_list() if k is not np.nan]))
                            else:
                                value, valueType = [], []
                            if item:
                                cea_list.append([filename, nrow, 0, item, itemType, 'Step 3', bestname])
                            if value:
                                cea_list.append([filename, nrow, col, value, valueType, 'Step 3', bestname])
                        except Exception:
                            pass
                except Exception:
                    pass

    # STEP 4 in the workflow
    if step4:
        # # MATCHING via the tail-entity-label and main-column-label
        for row in nomatch_row or []:
            for col in entity_columns or []:
                value_to_match = filecsv.iloc[row, col]
                if not isfloat(value_to_match) and not re.match(r"^(\d{4})/(\d{2})/(\d{2})$", value_to_match):
                    try:
                        WDitem = get_SPARQL_dataframe_item(value_to_match)
                        bestname = difflib.get_close_matches(filecsv.iloc[row, 0], WDitem.itemLabel.to_list(), n=2,
                                                             cutoff=0.95)
                        if len(bestname) == 0:
                            bestname = difflib.get_close_matches(filecsv.iloc[row, 0], WDitem.itemLabel.to_list(), n=2,
                                                                 cutoff=0.905)
                        if len(bestname) > 0:
                            WD = WDitem[WDitem.itemLabel.isin(bestname)]
                            item = list(set(WD.item.to_list()))
                            if 'itemType' in WD.columns:
                                itemType = list(set([k for k in WD.itemType.to_list() if k is not np.nan]))
                            else:
                                itemType = []
                            properties = [
                                x.replace("/prop/P", "/prop/direct/P").replace("/direct-normalized/", "/direct/") for x
                                in WD.p2.to_list()]
                            properties = list(set(zip(properties, WD.item.to_list())))
                            WD = WD[(~WD.value.str.contains('/statement/')) & (WD.value.str.contains('wikidata.org'))]
                            if not WD.empty:
                                value, valueType = list(set(WD.value.to_list())), list(
                                    set([k for k in WD.valueType.to_list() if k is not np.nan]))
                            else:
                                value, valueType = [], []
                            if properties and item:
                                cpa_list.append(
                                    [filename, row, 0, col, properties, item, itemType, 'tail-entity-label main-label',
                                     bestname])
                            if item:
                                cea_list.append([filename, row, 0, item, itemType, 'Step 4', bestname])
                            if value:
                                cea_list.append([filename, row, col, value, valueType, 'Step 4', bestname])
                    except Exception:
                        pass

    # MATCHING via column types in Steps 5 and 6
    if step5 or step6:
        # Estimate the types of columns in this table
        col_type = {}
        for row_type in cea_list[cea_ind:]:
            if len(row_type[4]) > 0:
                if col_type.get(row_type[2]):
                    col_type[row_type[2]].extend([etype.split('/')[-1] for etype in row_type[4]])
                else:
                    col_type[row_type[2]] = [etype.split('/')[-1] for etype in row_type[4]]
        col_type.update((key, [ct[0] for ct in Counter(value).most_common(2)]) for key, value in col_type.items())

    # STEP 5 in the workflow
    if step5:
        # We match tail-entities using its type and itemLabel.
        for nrow in nomatch_row or []:
            for ncol in entity_columns or []:
                try:
                    for column_type in col_type[ncol]:
                        WDtype = get_SPARQL_dataframe_type(filecsv.iloc[nrow, ncol], column_type)
                        item = list(set(WDtype.item.to_list()))
                        if item:
                            cea_list.append(
                                [filename, nrow, ncol, item, ["http://www.wikidata.org/entity/" + column_type],
                                 'Step 5', list(set(WDtype.itemLabel.to_list()))])
                except Exception:
                    pass

    # STEP 6 in the workflow
    if step6:
        # We match entities in the main column using its datatype
        if col_type.get(0) and len(nomatch_row) > 0:
            for column_type in col_type.get(0):
                try:
                    WDtype = get_SPARQL_dataframe_type2(column_type)
                    for row in nomatch_row or []:
                        proper_name = difflib.get_close_matches(filecsv.iloc[row, 0], WDtype.itemLabel.to_list(), n=15,
                                                                cutoff=0.95)
                        if len(proper_name) == 0:
                            proper_name = difflib.get_close_matches(filecsv.iloc[row, 0], WDtype.itemLabel.to_list(),
                                                                    n=15, cutoff=0.9)
                            if len(proper_name) == 0:
                                proper_name = difflib.get_close_matches(filecsv.iloc[row, 0],
                                                                        WDtype.itemLabel.to_list(), n=15, cutoff=0.8)
                                if len(proper_name) == 0:
                                    proper_name = difflib.get_close_matches(filecsv.iloc[row, 0],
                                                                            WDtype.itemLabel.to_list(), n=15,
                                                                            cutoff=0.7)
                        this_row_item = []
                        cpa_row_ind = len(cpa_list)
                        if len(proper_name) > 0:
                            test_list = []
                            for proper in proper_name:
                                e = get_SPARQL_dataframe(proper, extra='?itemLabel ')
                                if isinstance(e, pd.DataFrame):
                                    test_list.append(e)
                            if len(test_list) > 0:
                                WDdf = pd.concat(test_list)

                            if isinstance(WDdf, pd.DataFrame):
                                for col in range(1, cols):
                                    try:
                                        df = match(WDdf, filecsv.iloc[row, col])
                                        df_prop = df[df.p2.str.contains('http://www.wikidata.org/')]
                                        properties = [
                                            x.replace("/prop/P", "/prop/direct/P").replace("/direct-normalized/",
                                                                                           "/direct/") for x in
                                            df_prop.p2.to_list()]
                                        properties = list(set(zip(properties, df_prop.item.to_list())))
                                        item = list(set(df_prop.item.to_list()))
                                        if 'itemType' in df_prop.columns:
                                            itemType = list(
                                                set([k for k in df_prop.itemType.to_list() if k is not np.nan]))
                                        else:
                                            itemType = []
                                        df_value = df[(~df.value.str.contains('/statement/')) & (
                                            df.value.str.contains('wikidata.org'))]
                                        if not df_value.empty:
                                            value, valueType = list(set(df_value.value.to_list())), list(
                                                set([k for k in df_value.valueType.to_list() if k is not np.nan]))
                                        else:
                                            value, valueType = [], []
                                        if properties and item:
                                            cpa_list.append(
                                                [filename, row, 0, col, properties, item, itemType, 'Step 6',
                                                 list(set(df_prop.itemLabel.to_list()))])
                                        if item:
                                            cea_list.append([filename, row, 0, item, itemType, 'Step 6',
                                                             list(set(df_prop.itemLabel.to_list()))])
                                            this_row_item.extend(item)
                                        if value:
                                            cea_list.append([filename, row, col, value, valueType, 'Step 6',
                                                             list(set(df_value.itemLabel.to_list()))])
                                    except Exception:
                                        pass
                        # Take the most possible item for this row and remove the properties which are not taken from this item
                        if len(this_row_item) > 0:
                            this_row_item = Counter(this_row_item).most_common(1)[0][0]
                            for i, cpa_row in enumerate(cpa_list[cpa_row_ind:]):
                                if len(cpa_row[4]) > 0:
                                    cpa_list[cpa_row_ind + i][4] = [prop for prop in cpa_row[4] if
                                                                    prop[1] == this_row_item]
                except Exception:
                    pass
    return [cpa_list, cea_list, nomatch]


def postprocessing(cpa_list, cea_list, filelist=None, target_cpa=None, target_cea=None, target_cta=None, gui=False):
    """Postprocessing is performed for input lists cpa_list and cea_list.
    The target-dataframes are optional. If they are given,
    only target-annotations are returned in """
    # Create dataframe using the non-matched names from the main column (0), the corresponding filename and row
    # nm = pd.DataFrame(nomatch)
    # Create CPA-dataframe from the list and find the most frequent property
    bbw_cpa_few = pd.DataFrame(cpa_list, columns=['file', 'row', 'column0', 'column', 'property', 'item', 'itemType',
                                                  'how_matched', 'what_matched'])
    bbw_cpa_sub = bbw_cpa_few.groupby(['file', 'column0', 'column']).agg(
        {'property': lambda x: tuple(x)}).reset_index()  # because a list is unhashable
    bbw_cpa_sub['property'] = bbw_cpa_sub['property'].apply(lambda x: [y[0] for subx in x for y in subx])  # flatten
    bbw_cpa_sub['property'] = bbw_cpa_sub['property'].apply(lambda x: Counter(x).most_common(2))
    bbw_cpa_sub['property'] = bbw_cpa_sub['property'].apply(lambda x: None if len(x) == 0 else x[0][0])
    bbw_cpa_sub = bbw_cpa_sub.dropna()
    # Keep only the target columns for CPA-challenge
    if filelist and isinstance(target_cpa, pd.DataFrame):
        bbw_cpa_sub = pd.merge(
            target_cpa[target_cpa.file.isin(filelist)].astype({"file": str, "column0": int, "column": int}),
            bbw_cpa_sub.astype({"file": str, "column0": int, "column": int, "property": str}),
            on=['file', 'column0', 'column'], how='inner')
    # Create CEA-dataframe from the list and drop rows with None or empty lists
    bbw_few = pd.DataFrame(cea_list,
                           columns=['file', 'row', 'column', 'item', 'itemType', 'how_matched', 'what_matched'])
    # Prepare dataframe for CEA-challenge
    bbw_cea_sub = bbw_few.groupby(['file', 'row', 'column']).agg({'item': lambda x: tuple(x)}).reset_index()
    bbw_cea_sub['item'] = bbw_cea_sub['item'].apply(lambda x: [y for subx in x for y in subx])
    bbw_cea_sub['item'] = bbw_cea_sub['item'].apply(lambda x: Counter(x).most_common(2))
    bbw_cea_sub['item'] = bbw_cea_sub['item'].apply(lambda x: None if len(x) == 0 else x[0][0])
    bbw_cea_sub = bbw_cea_sub.dropna()
    # Keep only the target columns for CEA-challenge
    if filelist and isinstance(target_cea, pd.DataFrame):
        bbw_cea_sub = pd.merge(
            target_cea[target_cea.file.isin(filelist)].astype({"file": str, "row": int, "column": int}),
            bbw_cea_sub.astype({"file": str, "row": int, "column": int, "item": str}),
            on=['file', 'row', 'column'], how='inner')
    # Drop None-rows from bbw_few before getting itemType for CTA:
    bbw_few = bbw_few.dropna()
    bbw_few = bbw_few[bbw_few['itemType'].map(lambda x: len(x)) > 0]
    # Prepare dataframe for CTA-challenge
    bbw_cta_one = bbw_few.groupby(['file', 'column']).agg({'itemType': lambda x: tuple(x)}).reset_index()
    bbw_cta_one['itemType'] = bbw_cta_one['itemType'].apply(lambda x: [y for subx in x for y in subx])
    # bbw_cta_one['itemType'] = bbw_cta_one['itemType'].apply(lambda x: get_common_class(x) if len(x)>1 else x[0])
    bbw_cta_one['itemType'] = bbw_cta_one['itemType'].apply(lambda x: Counter(x).most_common(2))
    bbw_cta_one['itemType'] = bbw_cta_one['itemType'].apply(lambda x: get_one_class(x))
    bbw_cta_sub = bbw_cta_one.dropna()
    # Keep only the target columns for CTA-challenge
    if filelist and isinstance(target_cta, pd.DataFrame):
        bbw_cta_sub = pd.merge(target_cta[target_cta.file.isin(filelist)].astype({"file": str, "column": int}),
                               bbw_cta_sub.astype({"file": str, "column": int, "itemType": str}),
                               on=['file', 'column'], how='inner')
    # Print statistics
    if filelist and not gui:
        stat_cpa_matched = len(bbw_cpa_sub)
        if isinstance(target_cpa, pd.DataFrame):
            stat_cpa_target = len(target_cpa[target_cpa.file.isin(filelist)])
        stat_cea_matched = len(bbw_cea_sub)
        if isinstance(target_cea, pd.DataFrame):
            stat_cea_target = len(target_cea[target_cea.file.isin(filelist)])
        stat_cta_matched = len(bbw_cta_sub)
        if isinstance(target_cta, pd.DataFrame):
            stat_cta_target = len(target_cta[target_cta.file.isin(filelist)])
        print('\n*** Internal statistics ***')
        print('Task', 'Coverage', 'Matched', 'Total', 'Unmatched', sep='\t')
        try:
            print('CEA', round(stat_cea_matched / stat_cea_target, 4), stat_cea_matched, stat_cea_target,
                  stat_cea_target - stat_cea_matched, sep='\t')
        except Exception:
            pass
        try:
            print('CTA', round(stat_cta_matched / stat_cta_target, 4), stat_cta_matched, stat_cta_target,
                  stat_cta_target - stat_cta_matched, sep='\t')
        except Exception:
            pass
        try:
            print('CPA', round(stat_cpa_matched / stat_cpa_target, 4), stat_cpa_matched, stat_cpa_target,
                  stat_cpa_target - stat_cpa_matched, sep='\t')
        except Exception:
            pass
    return [bbw_cpa_sub, bbw_cea_sub, bbw_cta_sub]


def annotate(filecsv, filename=''):
    """
    Parameters
    ----------
    filecsv : pd.DataFrame
        Input dataframe.
    filename : str
        A filename.
    Returns
    -------
    list
        [
        bbwtable - dataframe containing annotated Web Table,
        urltable - dataframe containing URLs of annotations,
        labeltable - dataframe containing labels of annotations,
        cpa_sub - dataframe with annotations for CPA task,
        cea_sub - dataframe with annotations for CEA task,
        cta_sub - dataframe with annotations for CTA task
        ].

    """
    filename = filename.replace('.csv', '')
    cpa, cea, nomatch = [], [], []
    filecsv = preprocessing(filecsv)
    [cpa, cea, nomatch] = contextual_matching(filecsv, filename, cpa, cea, nomatch,
                                              step3=False, step4=False, step5=True, step6=True)
    [cpa_sub, cea_sub, cta_sub] = postprocessing(cpa, cea, [filecsv], gui=True)
    bbwtable = filecsv
    urltable = pd.DataFrame(columns=filecsv.columns)
    labeltable = pd.DataFrame(columns=filecsv.columns)
    if not cea_sub.empty:
        for row in set(cea_sub.row.to_list()) or []:
            for column in set(cea_sub.column.to_list()) or []:
                try:
                    link = cea_sub.item[(cea_sub.row == row) & (cea_sub.column == column)].to_list()[0]
                    if link:
                        label = get_wikidata_title(link)
                        urltable.loc[row, column] = link
                        labeltable.loc[row, column] = label
                        bbwtable.loc[row, column] = '<a target="_blank" href="' + link + '">' + label + '</a>'

                except Exception:
                    pass
    if not cpa_sub.empty:
        for column in set(cpa_sub.column.to_list()) or []:
            try:
                link = str(cpa_sub.property[cpa_sub.column == column].to_list()[0])
                label = get_wikidata_title(link)
                bbwtable.loc['index', column] = '<a target="_blank" href="' + link + '">' + label + '</a>'
                urltable.loc['index', column] = link
                labeltable.loc['index', column] = label
            except Exception:
                pass
    if not cta_sub.empty:
        for column in set(cta_sub.column.to_list()) or []:
            try:
                link = str(cta_sub.itemType[cta_sub.column == column].to_list()[0])
                label = get_wikidata_title(link)
                bbwtable.loc['type', column] = '<a target="_blank" href="' + link + '">' + label + '</a>'
                urltable.loc['type', column] = link
                labeltable.loc['type', column] = label
            except Exception:
                pass
    bbwtable = bbwtable.rename(index={'index': 'property'})
    bbwtable = bbwtable.replace({np.nan: ''})
    bbwtable.columns = bbwtable.iloc[0]
    bbwtable = bbwtable[1:]
    urltable = urltable.rename(index={'index': 'property'})
    urltable = urltable.replace({np.nan: ''})
    urltable.columns = bbwtable.columns
    labeltable = labeltable.rename(index={'index': 'property'})
    labeltable = labeltable.replace({np.nan: ''})
    labeltable.columns = bbwtable.columns
    return [bbwtable, urltable, labeltable, cpa_sub, cea_sub, cta_sub]

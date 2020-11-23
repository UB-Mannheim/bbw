# bbw (boosted by wiki)
[![PyPI version](https://badge.fury.io/py/bbw.svg)](https://badge.fury.io/py/bbw)
[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/UB-Mannheim/bbw/main?filepath=bbw.ipynb)
[![Language grade: Python](https://img.shields.io/lgtm/grade/python/g/UB-Mannheim/bbw.svg?logo=lgtm&logoWidth=18)](https://lgtm.com/projects/g/UB-Mannheim/bbw/context:python)

* Annotates tabular data with the entities, types and properties in [Wikidata](https://www.wikidata.org).
* Easy to use: bbw.annotate().
* Resolves even tricky spelling mistakes via meta-lookup through [SearX](https://github.com/searx/searx).
* Matches to the up-to-date values in Wikidata without the dump files.
* Ranked in third place at [SemTab2020](https://www.cs.ox.ac.uk/isg/challenges/sem-tab/2020).

## Table of contents
- [How to use](#how-to-use)
- [Installation](#installation)
- [Citing](#citing)
- [SemTab2020](#semtab2020)

## How to use

### Import library
```python
from bbw import bbw
```

The easiest way to annotate the dataframe Y is:
```python
[web_table, url_table, label_table, cpa, cea, cta] = bbw.annotate(Y)
```
It returns a list of six dataframes. The first three dataframes contain the annotations in the form of HTML-links, URLs and labels of the entities in Wikidata correspondingly. The dataframes have two more rows than Y. These two rows contain the annotations for types and properties. The last three dataframes contain the annotations in the format required by [SemTab2020](https://www.cs.ox.ac.uk/isg/challenges/sem-tab/2020) challenge.

The fastest way to annotate the dataframe Y is:
```python
[cpa_list, cea_list, nomatch] = bbw.contextual_matching(bbw.preprocessing(Y))
[cpa, cea, cta] = bbw.postprocessing(cpa_list, cea_list)
```
The dataframes ```cpa```, ```cea``` and ```cta``` contain the annotations in [SemTab2020](https://www.cs.ox.ac.uk/isg/challenges/sem-tab/2020)-format. The list ```nomatch``` contains the labels which are not matched. The unprocessed and possibly non-unique annotations are in the lists ```cpa_list``` and ```cea_list```.

### GUI

If you need to annotate only one table, use the simple GUI:
```shell
streamlit run bbw_gui.py
```

Open the browser at http://localhost:8501 and choose a CSV-file. The annotation process starts automatically. It outputs the six tables of the annotate function.

Try it out online (no SearX support) with this [binder link](https://mybinder.org/v2/gh/UB-Mannheim/bbw/main?urlpath=proxy/8501/).

### CLI

If you need to annotate a few tables, use the CLI-tool:
```shell
python3 bbw_cli.py --amount 100 --offset 0
```
### GNU parallel

If you need to annotate hundreds or thousands of tables, use the script with GNU parallel:
```shell
./bbw_parallel.py
```

## Installation

You can use pip to install bbw:
```
pip install bbw
```

The latest version can be installed directly from github:
```
pip install git+https://github.com/UB-Mannheim/bbw
```

You can test bbw in a virtual environment:
```
pip install virtualenv
virtualenv testing_bbw
source testing_bbw/bin/activate
python
from bbw import bbw
[web_table, url_table, label_table, cpa, cea, cta] = bbw.annotate(bbw.pd.DataFrame([['0','1'],['Mannheim','Rhine']]))
print(web_table)
deactivate
```

Install also [SearX](https://github.com/searx/searx), because bbw meta-lookups through it. 
```shell
export PORT=80
docker pull searx/searx
docker run --rm -d -v ${PWD}/searx:/etc/searx -p $PORT:8080 -e BASE_URL=http://localhost:$PORT/ searx/searx
```
SearX is running on http://localhost:80. bbw sends GET requests to it.

## Citing

If you find bbw useful in your work, a proper reference would be:
```
@inproceedings{2020_bbw,
  author    = {Renat Shigapov and Philipp Zumstein and Jan Kamlah and Lars Oberl{\"a}nder and J{\"o}rg Mechnich and Irene Schumm},
  title     = {bbw: {M}atching {CSV} to {W}ikidata via {M}eta-lookup},
  booktitle = {SemTab@ISWC 2020},
  year = {2020}
}
```

## SemTab2020

The library was designed, implemented and tested during [SemTab2020](https://www.cs.ox.ac.uk/isg/challenges/sem-tab/2020).
It received the best scores in the last 4th round at automatically generated dataset:

| Task | F1-score | Precision | Rank |
|:------:|:------:|:------:|:------:|
| CPA | 0.995 | 0.996 | 2 |
| CTA | 0.980 | 0.980 | 2 |
| CEA | 0.978 | 0.984 | 4 |

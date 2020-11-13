# bbw

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
It returns a list of six dataframes. The first three dataframes contain annotations in the form of HTML-links, URLs and labels of the entities in Wikidata correspondingly. The dataframes have two more rows than Y. These two rows contain annotations for types and properties. The last three dataframes contain the annotations in the format required by [SemTab2020](https://www.cs.ox.ac.uk/isg/challenges/sem-tab/2020) challenge.

### GUI

If you need to annotate only one table, use the simple GUI:
```shell
streamlit run bbw_gui.py
```

Open the browser at http://localhost:8501 and choose a CSV-file. The annotation process starts automatically. It outputs the six tables of the annotate function.

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

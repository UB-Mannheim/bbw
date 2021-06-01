# bbw docs

*bbw* is a semantic annotator "*b*oosted *b*y *w*iki" for linking tabular data without metadata to a [Wikibase](https://wikiba.se) instance (e.g., [Wikidata](https://www.wikidata.org)) via contextual matching and meta-lookup (metasearch).

* Annotates tabular data with the entities, types and properties in [Wikidata](https://www.wikidata.org).
* Easy to use: `bbw.annotate()`.
* Resolves even tricky spelling mistakes via meta-lookup through the [SearX](https://github.com/searx/searx) metasearch engine.
* Matches to the up-to-date values in [Wikidata](https://www.wikidata.org) without the dump files.
* Ranked in third place at [SemTab2020](https://www.cs.ox.ac.uk/isg/challenges/sem-tab/2020).

## Installation

You can use pip to install *bbw*:
```
pip install bbw
```

The latest version can be installed directly from github:
```
pip install git+https://github.com/UB-Mannheim/bbw
```

Install also [SearX](https://github.com/searx/searx), because *bbw* meta-lookups through it. 
```shell
export PORT=80
docker pull searx/searx
docker run --rm -d -v ${PWD}/searx:/etc/searx -p $PORT:8080 -e BASE_URL=http://localhost:$PORT/ searx/searx
```
SearX is running on http://localhost:80. *bbw* sends GET requests to it.
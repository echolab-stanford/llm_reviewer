# LLM Reviewer 📚🤖

<img align="right" src="figures/logo_cute.png alt="image" />
This library is an attempt at building a semi-automatic system to explore scientific literature. We want to achieve several goals: 
 - *Literature recommendation based on paper embeddings*: This can do vanilla topic-based, but also methods-based (i.e. what is the *causal* evidence of adaptation to the effects of temperature on mortality).
 - *QA Tasks*: Given a body of literature as a sequence of PDFs, what do we know about a particular topic? What is the summary of the evidence? 
 - *Topic classification*: What are the clusters in the literature? How can we find these? 

We currently use ~two~ one main source of paper information, [Crossref][1] to get paper information. Crossref is a reliable source for DOI, title and author data, but is not complete for all relevant fields, such as paper abstracts or full-text search. This library has the ability to parse the original JSON data and create a query-based SQL (DuckDB) table to store the information and create BERT-like embeddings for the abstract. 

## How to install 

You can create a sample mamba environment using: 

```bash
mamba create -n llm_review python=3.12
```

and then from the root of this repo, install the library with edits as: 

```bash
pip install -e .
```

## How to use

The main point of interaction is a command-line tool:

```
 Usage: adaptreview [OPTIONS] COMMAND [ARGS]...

 Tool to review adaptation papers

╭─ Options ─────────────────────────────────────────────────────────────────────────────────────────────╮
│ --install-completion          Install completion for the current shell.                               │
│ --show-completion             Show completion for the current shell, to copy it or customize the      │
│                               installation.                                                           │
│ --help                        Show this message and exit.                                             │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────╯
╭─ Commands ────────────────────────────────────────────────────────────────────────────────────────────╮
│ transform-crossref   Transform a list of DOIs records from JSON into a parquet files                  │
│ create-db            Create a DuckDB database with DOI records from Parquet files                     │
│ embed-abstracts      Use a LLM to embed the abstracts of DOI records the database                     │
│ download             Download papers from the DOIs in the database                                    │
╰───────────────────────────────────────────────────────────────────────────────────────────────────────╯
```


[1]: https://www.crossref.org/
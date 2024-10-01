# Use LLMs and open-source literature databases to build meta-literature reviews

# Main goals
In this repo we have three four goals: 
 - Retrieve candidate papers from the literature using a search string
 - Download papers from candidate DOIs and store them locally
 - Pass both the paper metadata and PDF data to classify the paper
 - Summarize and build a literature review.

# How? 
We use Crossref (CF) and store the data locally to avoid having to do many API 
calls and pagination. To do this we download the complete CF dataset and use
DuckDB to process the data (god bless SQL). We use the DOIs to retrieve the 
PDF for such paper from SciHub 🏴‍☠️ .

We plan on build a nice pipeline in LangChain to build the LLM part. 

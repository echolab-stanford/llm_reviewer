from paperqa import Settings, ask, search_query


settings = Settings()
settings.llm="gpt-3.5-turbo"
settings.summary_llm="gpt-3.5-turbo"
settings.paper_directory = "pdf"
settings.answer.answer_max_sources = 3
settings.answer.evidence_k = 5



answer = ask(
    "What are the statistical methods used on these set of papers?",
    settings=settings
)

answer_causal = ask(
    "Of the reviewed papers, which ones have strong causal evidence. This is defined as RCTs or observational causal inference methods",
    settings=Settings(
        llm="gpt-3.5-turbo", summary_llm="gpt-3.5-turbo", paper_directory="pdf"
    ),
)

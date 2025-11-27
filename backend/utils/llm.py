from langchain_openai import AzureOpenAIEmbeddings


def get_embeddings(model='text-embedding-3-large'):
    return AzureOpenAIEmbeddings(model=model)



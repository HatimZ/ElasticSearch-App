from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient


class es_practice_hz:

    def __init__(self):
        self.es_client = Elasticsearch("localhost:9200" , http_auth = ["elastic" , "hatim"],timeout =30)

        self.index_client = IndicesClient(self.es_client)

        self.configurations = {
            "settings": {
                "index": {"number_of_replicas": 2},
                "analysis": {
                    "filter": {
                        "ngram_filter": {
                            "type": "edge_ngram",
                            "min_gram": 2,
                            "max_gram": 15,
                        },
                    },
                    "analyzer": {
                        "ngram_analyzer": {
                            "type": "custom",
                            "tokenizer": "standard",
                            "filter": ["lowercase", "ngram_filter"],
                        },
                    },
                },
            },
            "mappings": {
                "properties": {
                    "id": {"type": "long"},
                    "name": {
                        "type": "text",
                        "analyzer": "standard",
                        "fields": {
                            "keyword": {"type": "keyword"},
                            "ngrams": {"type": "text", "analyzer": "ngram_analyzer"},
                        },
                    },
                    "brand": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"},
                        },
                    },
                    "price": {"type": "float"},
                    "attributes": {
                        "type": "nested",
                        "properties": {
                            "attribute_name": {"type": "text"},
                            "attribute_value": {"type": "text"},
                        },
                    },
                }
            },
        }

    def create_index(self , index_name , config_file):
        self.index_client.create(index = index_name , body = config_file)

    def create_document(self, doc_Name , data , doc_id):
        self.es_client.index(index = doc_Name , id = doc_id , body = data)

    def bulk_create(self,bulk_data):
        self.es_client.bulk(body="\n".join(bulk_data))

    def search_data(self):
        return self.es_client
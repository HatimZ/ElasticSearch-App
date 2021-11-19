from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient


class es_practice_hz:

    index_name = ""
   
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

    def create_index(self , ind_name ):
        self.index_client.create(index = ind_name , body = self.configurations)
        self.index_name = ind_name

    def create_document(self, data , doc_id):
        self.es_client.index(index = self.index_name , id = doc_id , body = data)

    def bulk_doc_create(self,bulk_data , index_name):
        self.es_client.bulk(body="\n".join(bulk_data))
        self.index_name = index_name

    def search_data(self, index_name , query):
        return self.es_client.search(index =index_name , body = query)

    def count_docs(self ,  query):
        self.es_client.count(index = self.index_name , body= query)

    def delete_doc(self  , doc_id  ):
        self.es_client.delete(index = self.index_name , doc_id= doc_id )

    def delete_index(self , ind_name):
        self.es_client.indices.delete(index= ind_name, ignore=[400, 404])    

    def list_allIndex(self):
        for index in self.es_client.indices.get('*'):
            print (index)

    def delete_Allindex(self):
        self.es_client.indices.delete('*')  
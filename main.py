from elasticsearch import Elasticsearch
import warnings
from elasticsearch import helpers
import time
warnings.filterwarnings("ignore")

es = Elasticsearch(['https://ec2-18-219-23-248.us-east-2.compute.amazonaws.com:9200'], http_auth=('elastic', 'xlvF=Sn5Nh*uAs9y-5S2'), use_ssl=True, verify_certs=False)

# Creating an index
def creating_index(es, index):
    es.indices.create(index=index, body={
    'settings' : {
         'index' : {
            "number_of_shards": 2,
            "number_of_replicas": 0
         }
    }
})

# Indexing documents
def indexing_documents(es, index):
    documents_json_path = "/Users/chensun/GDPR_RTBF_project/testData_wiki/products-bulk.json"

    # step 1: create index with sepecfic setting or mapping
    creating_index(es, index)

    # step 2: inserting documents
    # change the format of json file to every line is a json object (ignore the object lines in "products-bulk.json")
    with open(documents_json_path) as json_file:
        body_tmp = json_file.readlines()
    body = []
    for i in range(0, len(body_tmp)):
        if i % 2:
            body.append(body_tmp[i])
    helpers.bulk(es, actions=body, index=index)
    print(len(body))

# search a query among index (full text search)
def searching_documents(es, index):

    # data = es.search(index=index, body={"query": {"match": { "name": "Wine - Maipo Valle Cabernet"}}})
    data = es.search(index=index, body= {"query":{
         "term": {
           "name.keyword": "Chorizo and sausage pasta"}}})
    for hit in data['hits']['hits']:
        print(hit["_score"])
        print(hit["_source"])

# delete by query (matching the exact term)
def delete_by_query(es, index):
    response = es.delete_by_query(index=index, body={"query": {"term": {"name.keyword": "Chorizo and sausage pasta"}}})
    print("response: ", response)

# delete an index
def deleting_index(es, index):
    es.indices.delete(index=index)

# update by query (matching the exact term)
def update_by_query(es, index):
    body =  {
    "script" : "ctx._source.remove('in_stock')",
    "query": {
        "bool": {
            "must": [
                {"exists":
                     {"field": "in_stock"}},
                { "term":
                    {"name.keyword": "Chorizo and sausage pasta"}}
            ]
        }
    }
    }
    response = es.update_by_query(body=body, index=index, conflicts='proceed')
    time.sleep(3)
    print(response)


if __name__ == "__main__":
    index_name = 'products'
    #indexing_documents(es,index_name)
    #update_by_query(es,index_name)
    #searching_documents(es, index_name)
    #delete_by_query(es, index_name)
    #deleting_index(es, index_name)


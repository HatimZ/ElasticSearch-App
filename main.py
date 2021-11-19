
from es_practice import es_practice_hz
from CSVtoJSON import formatConverter

# doc3 = {
#         "id" : 1 ,
#         "name" : "Dell Inspiron 5500" , 
#         "brand" : "HP" ,
#         "price": 50000,
#         "attributes" : [
#             {"attribute_name": "cpu" , "attribute_value" : "Intel Core i5"
#             },
#             {"attribute_name":"RAM" , "attribute_value":"8GB" },
#             {"attribute_name":"storage" , "attribute_value":"500GB" },],
#     }

new_database = es_practice_hz()  #creating a client and connecting to the server, if we run this
                              #multiple times no problem as only multiple clients would be created.

# helperClass = formatConverter(["id", "name", "price", "brand", "cpu", "memory", "storage"], "dell_laptop")
# converted_data = helperClass.convert_format("laptops_data.csv")
# new_database.bulk_doc_create(converted_data , "dell_laptop")

search_query1 = {
    "query": {
        "match":{
            "brand" : "HP"
            
        }
    }
}

search_query2 = {
  "query": {
    "range" : { 
      "price": {
        "gte": 50000,
        "lte": 100000
      }
  }
  }
}

search_query3 = {
  "query": {
    "bool": {
      "must": [
        {
          "match": {
            "brand": "HP"
            
          }
        }
      ]
      , "should": [
        {"match": {
          "attribute_value": "Intel Core i7"
        }
          
        },
         
      ]
      , "filter": [ {
       "range" : { 
      "price": {
        "gte": 50000,
        "lte": 100000
      }
       }
      }
      ]
      
    }}}
  

search_query4 = {
    "query": {
        "match":{
            "brand" : "HP"
            
        }
    }
}
#new_database.delete_index("dell_laptop")
new_database.list_allIndex()

print (new_database.search_data(  "dell_laptop" ,search_query4))
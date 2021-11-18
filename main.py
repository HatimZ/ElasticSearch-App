
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

#helperClass = formatConverter(["id", "name", "price", "brand", "cpu", "memory", "storage"], "dell_laptop")
#converted_data = helperClass.convert_format("laptops_data.csv")
#new_database.bulk_create(converted_data)

search_query = {
    "query": {
        "match":{
            "brand" : "HP"
            
        }
    }


}

print (new_database.search_data().search(index = "dell_laptop" , body = search_query))
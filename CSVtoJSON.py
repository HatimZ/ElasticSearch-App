import csv
import json

from es_practice import es_practice_hz


class formatConverter(es_practice_hz):

    def __init__(self, columns, index_name ):
        self.columns = columns
        self.index_name = index_name


    def convert_format(self, file_name):
        with open( file_name, "r") as fi:
            reader = csv.DictReader(
                fi, fieldnames= self.columns, delimiter=",", quotechar='"'
            )

            # This skips the first row which is the header of the CSV file.
            next(reader)

            actions = []
            for row in reader:
                action = {"index": {"_index": self.index_name, "_id": int(row["id"])}}
                doc = {
                    "id": int(row["id"]),
                    "name": row["name"],
                    "price": float(row["price"]),
                    "brand": row["brand"],
                    "attributes": [
                        {"attribute_name": "cpu", "attribute_value": row["cpu"]},
                        {"attribute_name": "memory", "attribute_value": row["memory"]},
                        {
                            "attribute_name": "storage",
                            "attribute_value": row["storage"],
                        },
                    ],
                }
                actions.append(json.dumps(action))
                actions.append(json.dumps(doc))

        with open("laptops_demo.json", "w") as fo:
            fo.write("\n".join(actions))

        return actions

   
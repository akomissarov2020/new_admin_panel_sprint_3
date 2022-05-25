from elasticsearch import Elasticsearch

es = Elasticsearch()

indexes = es.indices.get_alias()

for ind in indexes:
	print(ind, es.count(index=ind))

item = es.search(index="persons", size=2, from_=4)
print(len(item["hits"]["hits"]))
print(item["hits"]["hits"][0])
print(item["hits"]["hits"][1])
	
# print(es.get(id="5c4ed86e-021e-44ef-b899-5db849ed343f", index="movies"))


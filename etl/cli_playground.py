from elasticsearch import Elasticsearch

es = Elasticsearch()

indexes = es.indices.get_alias()

for ind in indexes:
	print(ind, es.count(index=ind))

item = es.search(index="movies")
print(item["hits"])
	
print(es.get(id="5c4ed86e-021e-44ef-b899-5db849ed343f", index="movies"))


from elasticsearch import Elasticsearch

es = Elasticsearch()

indexes = es.indices.get_alias()

for ind in indexes:
	print(ind, es.count(index=ind))

item = es.search(index="movies")
print(item["hits"])
	
print(es.get(id="cd19b384-babd-4b0c-ba0a-5c272bcf0238", index="movies"))


one or two paragraphs

build an index for better performance

largest performance hit is in REGEX vulnerability - text search index (Full Text Search index)
https://docs.pinot.apache.org/basics/indexing/text-search-support#enable-a-per-column-text-index

```SQL
SELECT source_ip, COUNT(*) AS match_count FROM ingest_kafka
WHERE
  TEXT_MATCH(content, 'vulnerability') AND severity = 'High'
GROUP BY source_ip
ORDER BY match_count DESC
```

Wine: PULL queries OLAP we pull the data
we have to periodically pull the records than get the results
-> users can do ad-hoc queries 


Stream processors PUSH queries
as soon as query is met pushes result
-> i.e. get data for fraud detection, is the same query all the time and we want results
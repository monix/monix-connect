---
id: es
title: Elasticsearch
---

## Introduction

_Elasticsearch_ ([ES](https://www.elastic.co/elasticsearch)) is a distributed, RESTful search and analytics engine 
capable of addressing a growing number of use cases. It can now be easily integrated with monix.
   
## Dependency
 
 Add the following dependency:
 
 ```scala
 libraryDependencies += "io.monix" %% "monix-es" % "0.5.0"
 ```
 
## Client
 
 This connector uses the _underlying_ `ElasticClient` from the [elastic4s](https://github.com/sksamuel/elastic4s),
 it is a concise, idiomatic, reactive, type safe Scala client for Elasticsearch. We use _cats effect_ to acquire and
 release the resource (`ElasticClient`).

 Example:

 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import monix.connect.es.Elasticsearch
import monix.eval.Task

val esResource: Resource[Task, Elasticsearch] = Elasticsearch.create("http://localhost:9200")
```
##Create index 

 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Response
import com.sksamuel.elastic4s.requests.indexes.CreateIndexResponse
import monix.connect.es.Elasticsearch
import monix.eval.Task

val esResource: Resource[Task, Elasticsearch] = Elasticsearch.create("http://localhost:9200")

val indexName = "test_index"
val indexSource = """{"settings":{"number_of_shards":1},"mappings":{"properties":{"a":{"type":"text"} } } }"""
val createIndexRequest = createIndex(indexName).source(indexSource)

val task: Task[Response[CreateIndexResponse]] = esResource.use { es =>
    es.createIndex(createIndexRequest)
}
```
##Get index 

 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Response
import com.sksamuel.elastic4s.requests.indexes.GetIndexResponse
import monix.connect.es.Elasticsearch
import monix.eval.Task

val esResource: Resource[Task, Elasticsearch] = Elasticsearch.create("http://localhost:9200")

val indexName = "test_index"
val getIndexRequest = getIndex(indexName)

val task: Task[Response[Map[String, GetIndexResponse]]] = esResource.use { es =>
    es.getIndex(getIndexRequest)
}
```

##Delete index 

 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Response
import com.sksamuel.elastic4s.requests.indexes.admin.DeleteIndexResponse
import monix.connect.es.Elasticsearch
import monix.eval.Task

val esResource: Resource[Task, Elasticsearch] = Elasticsearch.create("http://localhost:9200")

val indexName = "test_index"
val deleteIndexRequest = deleteIndex(indexName)

val task: Task[Response[DeleteIndexResponse]] = esResource.use { es =>
    es.deleteIndex(deleteIndexRequest)
}
```

## Update a doc in the index 
 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Response
import com.sksamuel.elastic4s.requests.update.UpdateResponse
import monix.connect.es.Elasticsearch
import monix.eval.Task

val esResource: Resource[Task, Elasticsearch] = Elasticsearch.create("http://localhost:9200")

val indexName = "test_index"
val id = "test_id"
val doc = """{"a":"test"}"""
val updateRequest = updateById(indexName, id).docAsUpsert(doc)

val task: Task[Response[UpdateResponse]] = esResource.use { es =>
    es.singleUpdate(updateRequest)
}
```

## Get a doc from the index by id 
 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Response
import com.sksamuel.elastic4s.requests.get.GetResponse
import monix.connect.es.`Elasticsearch`
import monix.eval.Task

val esResource: Resource[Task, Elasticsearch] = Elasticsearch.create("http://localhost:9200")

val indexName = "test_index"
val id = "test_id"
val doc = """{"a":"test"}"""

val getByIdRequest = get(indexName, id)
val task: Task[Response[GetResponse]] = esResource.use { es =>
    es.getById(getByIdRequest)
}

```

## Delete a doc in the index
 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import monix.connect.es.Elasticsearch
import monix.eval.Task

val esResource: Resource[Task, Elasticsearch] = Elasticsearch.create("http://localhost:9200")

val indexName = "test_index"
val id = "test_id"

val deleteRequest = deleteById(indexName, id)
val task = esResource.use { es =>
    es.singleDeleteById(deleteRequest)
}
```

## Count docs in the index
 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Response
import com.sksamuel.elastic4s.Indexes
import com.sksamuel.elastic4s.requests.count.CountResponse
import monix.connect.es.Elasticsearch
import monix.eval.Task

val esResource: Resource[Task, Elasticsearch] = Elasticsearch.create("http://localhost:9200")

val indexName = "test_index"
val countRequest = count(Indexes(indexName)).query(matchAllQuery())

val task: Task[Response[CountResponse]] = esResource.use { es =>
    es.singleCount(countRequest)
}
```

## Execute bulk requests
 
 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Response
import com.sksamuel.elastic4s.requests.bulk.BulkResponse
import monix.connect.es.Elasticsearch
import monix.eval.Task

val esResource: Resource[Task, Elasticsearch] = Elasticsearch.create("http://localhost:9200")

val indexName = "test_index"
val id = "test_id"
val doc = """{"a":"test"}"""

val updateRequest = updateById(indexName, id).docAsUpsert(doc)
val deleteRequest = deleteById(indexName, id)

val task: Task[Response[BulkResponse]] = esResource.use { es =>
    es.bulkExecuteRequest(Seq(updateRequest, deleteRequest))
}
```

## Search
 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Response
import com.sksamuel.elastic4s.requests.searches.SearchResponse
import monix.connect.es.Elasticsearch
import monix.eval.Task

val esResource: Resource[Task, Elasticsearch] = Elasticsearch.create("http://localhost:9200")

val indexName = "test_index"
val searchRequest = search(indexName).query(matchAllQuery())

val task: Task[Response[SearchResponse]] = esResource.use { es =>
    es.search(searchRequest)
}
```

## Scroll (ElasticsearchSource) 
Create a `Observable` object for retrieving large numbers of results (or even all results) from a single search request.

 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.searches.SearchHit
import monix.connect.es.Elasticsearch
import monix.eval.Task

val esResource: Resource[Task, Elasticsearch] = Elasticsearch.create("http://localhost:9200")

val indexName = "test_index"
val searchRequest = search(indexName).query(matchAllQuery()).keepAlive("1m")

val observable: Task[List[SearchHit]] = esResource.use { es =>
    es.scroll(searchRequest)
      .toListL
}
```

## Consume requests from the source (ElasticsearchSink)
Create a `Consumer` object for consuming requests from the source.

 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import monix.connect.es.Elasticsearch
import monix.eval.Task
import monix.reactive.Observable

val esResource: Resource[Task, Elasticsearch] = Elasticsearch.create("http://localhost:9200")

val indexName = "test_index"
val indexSource = """{"settings":{"number_of_shards":1},"mappings":{"properties":{"a":{"type":"text"} } } }"""
val id = "test_id"
val doc = """{"a":"test"}"""

val updateRequest = updateById(indexName, id).docAsUpsert(doc)
val deleteRequest = deleteById(indexName, id)
val observable = 
    Observable
      .from(Seq(updateRequest, deleteRequest))
      .bufferTumbling(2)

esResource.use { es =>
    observable
      .consumeWith(es.createBulkRequestsSink())
}
```

## Local testing

There is actually a very good support on regards to testing `Elasticsearch` locally.
 
### docker

 A local _Elasticsearch_ available as a docker image.
 
 You would just need to define it as a service in your `docker-compose.yml`:
 
 ```yaml
  elasticsearch:
    image: elasticsearch:7.9.3
    environment:
      discovery.type: single-node
    ports:
      - 9200:9200
    healthcheck:
      test: [ "CMD", "curl", "-f", "http://localhost:9200" ]
      interval: 30s
      timeout: 10s
      retries: 5
``` 

 Then, execute the following command to build and run the _elasticsearch_ image:
 
 ```shell script
 docker-compose -f ./docker-compose.yml up -d elasticsearch
```

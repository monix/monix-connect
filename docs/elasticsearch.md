---
id: elasticsearch
title: Elasticsearch
---

## Introduction

[_Elasticsearch_](https://www.elastic.co/elasticsearch) is a distributed, _RESTful_ search and analytics engine 
capable of addressing a growing number of use cases. Which now can easily be integrated with _Monix_, providing 
a functional api for use cases can come from executing single operations (_create_, _get_ and _delete_ index or _get_, _update_, _count_ docs and more...),
but also to _search_ or _upload_ in a reactive fashion with _Monix Reactive_.
   
## Dependency
 
 Add the following dependency:
 
 ```scala
 libraryDependencies += "io.monix" %% "monix-elasticsearch" % "0.5.0"
 ```
 
## Client
 This connector has been built on top of the `ElasticClient` from [elastic4s](https://github.com/sksamuel/elastic4s),
 which expoeses a pure, idiomatic, non-blocking and reactive api to interact with _Elasticsearch_.  
  
 You can find different builders to create the client under `monix.connect.elasticsearch.Elasticsearch`,
  which we will show on continuation.   


### Create

This builder is the recommended way to create the client, since it exposes a pure implementation that is backed 
 by the _Cats Effect_ [Resource](https://typelevel.org/cats-effect/datatypes/resource.html). 
 
 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import monix.connect.elasticsearch.Elasticsearch
import monix.eval.Task

val elasticsearchUrl = "http://localhost:9200"
val esResource: Resource[Task, Elasticsearch] = Elasticsearch.create(elasticsearchUrl)
```
### Unsafe create
Alternatively, you can create the 
 ```scala
import monix.connect.elasticsearch.Elasticsearch
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.{ElasticProperties, HttpClient}
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s._
import monix.eval.Task

val uri = "http://localhost:9200"
val elasticProperties = ElasticProperties(uri) // here different options could be set
val httpClient = JavaClient(elasticProperties)
val elasticsearch: Elasticsearch = Elasticsearch.createUnsafe(ElasticClient(client = httpClient))
```

## Single operation

### Create index 

 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Response
import com.sksamuel.elastic4s.requests.indexes.CreateIndexResponse
import monix.connect.elasticsearch.Elasticsearch
import monix.eval.Task

val esResource: Resource[Task, Elasticsearch] = Elasticsearch.create("http://localhost:9200")

val indexName = "my_index"
val indexSource = """{"settings":{"number_of_shards":1},"mappings":{"properties":{"a":{"type":"text"} } } }"""
val createIndexRequest = createIndex(indexName).source(indexSource)

val task: Task[Response[CreateIndexResponse]] = esResource.use { es =>
    es.createIndex(createIndexRequest)
}
```

### Get index 

 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Response
import com.sksamuel.elastic4s.requests.indexes.GetIndexResponse
import monix.connect.elasticsearch.Elasticsearch
import monix.eval.Task

val esResource: Resource[Task, Elasticsearch] = Elasticsearch.create("http://localhost:9200")

val indexName = "my_index"
val getIndexRequest = getIndex(indexName)

val task: Task[Response[Map[String, GetIndexResponse]]] = esResource.use (_.getIndex(getIndexRequest))
```

### Delete index 

 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Response
import com.sksamuel.elastic4s.requests.indexes.admin.DeleteIndexResponse
import monix.connect.elasticsearch.Elasticsearch
import monix.eval.Task

val esResource: Resource[Task, Elasticsearch] = Elasticsearch.create("http://localhost:9200")

val indexName = "my_index"
val deleteIndexRequest = deleteIndex(indexName)

val task: Task[Response[DeleteIndexResponse]] = esResource.use(_.deleteIndex(deleteIndexRequest))
```

### Update a doc
 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Response
import com.sksamuel.elastic4s.requests.update.UpdateResponse
import monix.connect.elasticsearch.Elasticsearch
import monix.eval.Task

val elasticsearch: Elasticsearch

val indexName = "my_index"
val id = "test_id"
val doc = """{"a":"test"}"""
val updateRequest = updateById(indexName, id).docAsUpsert(doc)

val task: Task[Response[UpdateResponse]] = elasticsearch.singleUpdate(updateRequest)

```

### Get a doc by id 
 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Response
import com.sksamuel.elastic4s.requests.get.GetResponse
import monix.connect.elasticsearch.Elasticsearch
import monix.eval.Task

val elasticsearch: Elasticsearch

val indexName = "my_index"
val id = "test_id"
val doc = """{"a":"test"}"""

val getByIdRequest = get(indexName, id)
val t: Task[Response[GetResponse]] = elasticsearch.getById(getByIdRequest)

```

### Delete doc 
 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import monix.connect.elasticsearch.Elasticsearch
import monix.eval.Task

val elasticsearch: Elasticsearch

val indexName = "my_index"
val id = "test_id"

val deleteRequest = deleteById(indexName, id)
val t = elasticsearch.singleDeleteById(deleteRequest)
```

### Count docs 
 ```scala
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Response
import com.sksamuel.elastic4s.Indexes
import com.sksamuel.elastic4s.requests.count.CountResponse
import monix.connect.elasticsearch.Elasticsearch
import monix.eval.Task

val elasticsearch: Elasticsearch

val indexName = "my_index"
val countRequest = count(Indexes(indexName)).query(matchAllQuery())

val t: Task[Response[CountResponse]] = elasticsearch.singleCount(countRequest)
}
```

### Bulk requests
 
 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Response
import com.sksamuel.elastic4s.requests.bulk.BulkResponse
import monix.connect.elasticsearch.Elasticsearch
import monix.eval.Task

val elasticsearch: Elasticsearch

val indexName = "my_index"
val id = "test_id"
val doc = """{"a":"test"}"""

val updateRequest = updateById(indexName, id).docAsUpsert(doc)
val deleteRequest = deleteById(indexName, id)

val t: Task[Response[BulkResponse]] = elasticsearch.bulkExecuteRequest(Seq(updateRequest, deleteRequest))
```

### Search request

 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.Response
import com.sksamuel.elastic4s.requests.searches.SearchResponse
import monix.connect.elasticsearch.Elasticsearch
import monix.eval.Task

val elasticsearch: Elasticsearch

val indexName = "my_index"
val searchRequest = search(indexName).query(matchAllQuery())

val t: Task[Response[SearchResponse]] = elasticsearch.search(searchRequest)
```

## ElasticsearchSource

### Scroll

Used to retrieve large numbers of results (or even all results) of a search request, 
which performs safely with an `Observable` that emits `SearchHit`s.

 ```scala
import cats.effect.Resource
import com.sksamuel.elastic4s.ElasticDsl._
import com.sksamuel.elastic4s.requests.searches.SearchHit
import monix.connect.elasticsearch.Elasticsearch
import monix.reactive.Observable

val elasticsearch: Elasticsearch

val indexName = "my_index"
val searchRequest = search(indexName).query(matchAllQuery()).keepAlive("1m")

val ob: Observable[SearchHit] = elasticsearch.scroll(searchRequest)
```

## ElasticsearchSink

### Consume bulk requests 

Creates a `Consumer` object that performs any type of elasticsearch requests emitted.

 ```scala
import com.sksamuel.elastic4s.ElasticDsl._
import monix.connect.elasticsearch.Elasticsearch
import monix.eval.Task
import monix.reactive.Observable

val elasticsearch: Elasticsearch

val indexName = "my_index"
val indexSource = """{"settings":{"number_of_shards":1},"mappings":{"properties":{"a":{"type":"text"} } } }"""
val id = "test_id"
val doc = """{"a":"test"}"""

val updateRequest = updateById(indexName, id).docAsUpsert(doc)
val deleteRequest = deleteById(indexName, id)

val t = Observable
          .now(Seq(updateRequest, deleteRequest))
          .consumeWith(elasticsearch.bulkRequestSink())     
```

## Local testing

In order to test `Elasticsearch` service locally, we would just need to use the `elasticsearch` image from DockerHub,
see in below snipped how it is being defined in the `docker-compose.yaml file.
 
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

Finally, refer to the port that the container is exposing:

```scala
import cats.effect.Resource
import monix.connect.elasticsearch.Elasticsearch
import monix.eval.Task

val elasticsearchUrl = "http://localhost:9200"
val resource: Resource[Task, Elasticsearch] = Elasticsearch.create("http://localhost:9200")
```

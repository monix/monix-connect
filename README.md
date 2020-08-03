# Monix Connect  

 [![release-badge][]][release] 
 [![workflow-badge][]][workflow] 
 [![Gitter](https://badges.gitter.im/monix/monix-connect.svg)](https://gitter.im/monix/monix-connect?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)
 [![Scala Steward badge](https://img.shields.io/badge/Scala_Steward-helping-blue.svg?style=flat&logo=data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAA4AAAAQCAMAAAARSr4IAAAAVFBMVEUAAACHjojlOy5NWlrKzcYRKjGFjIbp293YycuLa3pYY2LSqql4f3pCUFTgSjNodYRmcXUsPD/NTTbjRS+2jomhgnzNc223cGvZS0HaSD0XLjbaSjElhIr+AAAAAXRSTlMAQObYZgAAAHlJREFUCNdNyosOwyAIhWHAQS1Vt7a77/3fcxxdmv0xwmckutAR1nkm4ggbyEcg/wWmlGLDAA3oL50xi6fk5ffZ3E2E3QfZDCcCN2YtbEWZt+Drc6u6rlqv7Uk0LdKqqr5rk2UCRXOk0vmQKGfc94nOJyQjouF9H/wCc9gECEYfONoAAAAASUVORK5CYII=)](https://scala-steward.org)

 [workflow]:                https://github.com/monix/monix-connect/actions?query=branch%3Amaster+workflow%3Abuild
 [workflow-badge]:          https://github.com/monix/monix-connect/workflows/build/badge.svg

 [release]:                 https://search.maven.org/search?q=a:monix-connect*
 [release-badge]:           https://img.shields.io/github/v/tag/monix/monix-connect.svg
 
 ⚠️   _Mind that the project isn't yet stable, so binary compatibility is not guaranteed._
  
Monix Connect is an initiative to implement stream integrations for [Monix](https://monix.io/).

Learn more on how to get started in the [documentation page](https://monix.github.io/monix-connect/).

### Connectors

The below list comprehends the current set of available connectors:
1. [Akka](https://connect.monix.io/docs/akka)
2. [Apache Parquet](https://connect.monix.io/docs/parquet)
3. [AWS DynamoDB](https://connect.monix.io/docs/dynamodb)
4. [AWS S3](https://connect.monix.io/docs/s3)
5. [Hdfs](https://connect.monix.io/docs/hdfs)
6. [Google Cloud Storage](https://connect.monix.io/docs/gcs)
7. [Redis](https://connect.monix.io/docs/redis)

### Contributing

The Monix Connect project welcomes contributions from anybody wishing to
participate.  All code or documentation that is provided must be
licensed with the same license that Monix Connect is licensed with (Apache
2.0, see [LICENCE](./LICENSE)).

People are expected to follow the
[Scala Code of Conduct](./CODE_OF_CONDUCT.md) when
discussing Monix on GitHub, Gitter channel, or other venues.

Feel free to open an issue if you notice a bug, you have a question about the code,
 an idea for an existing connector or even for adding a new one. Pull requests are also
gladly accepted. For more information, check out the
[contributor guide](CONTRIBUTING.md).

## License

All code in this repository is licensed under the Apache License,
Version 2.0. See [LICENCE](./LICENSE).


# AWS DynamoDB Mock

[![Build and Tests](https://github.com/sviridov-alexey/aws-dynamodb-mock/actions/workflows/build.yml/badge.svg)](https://github.com/sviridov-alexey/aws-dynamodb-mock/actions/workflows/build.yml)

An implementation of Amazon's DynamoDB for usage in tests in JVM languages. Docker is not needed, it's a library that uses H2 to store data. It's in-memory and really easy to set up and start using.

## Implemented functionality of Dynamo
- `createTable`
- `deleteTable`
- `describeTable`
- `query`
- `scan`
- `putItem`
- `getItem`
- `updateItem`
- `deletetem`
- `batchWriteItem`
- Local secondary indexes

### Additional features
- `loadCSV` - allows you to put items in table that are stored in CSV format. [Example of file.](https://github.com/sviridov-alexey/aws-dynamodb-mock/blob/main/src/test/resources/load-items/basic.csv)

## How to use

## How to contribute 
- Open an issue to explain the issue you want to solve
- You can fork the project and open a PR

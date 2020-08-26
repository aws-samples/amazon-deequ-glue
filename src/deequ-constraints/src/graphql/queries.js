/* eslint-disable */
// this is an auto generated file. This will be overwritten

export const getDataQualitySuggestion = /* GraphQL */ `
  query GetDataQualitySuggestion($id: ID!) {
    getDataQualitySuggestion(id: $id) {
      id
      constraint
      constraintCode
      column
      enable
      tableHashKey
      createdAt
      updatedAt
    }
  }
`;
export const listDataQualitySuggestions = /* GraphQL */ `
  query ListDataQualitySuggestions(
    $filter: ModelDataQualitySuggestionFilterInput
    $limit: Int
    $nextToken: String
  ) {
    listDataQualitySuggestions(
      filter: $filter
      limit: $limit
      nextToken: $nextToken
    ) {
      items {
        id
        constraint
        constraintCode
        column
        enable
        tableHashKey
        createdAt
        updatedAt
      }
      nextToken
    }
  }
`;
export const getDataQualityAnalyzer = /* GraphQL */ `
  query GetDataQualityAnalyzer($id: ID!) {
    getDataQualityAnalyzer(id: $id) {
      id
      analyzerCode
      column
      enable
      tableHashKey
      createdAt
      updatedAt
    }
  }
`;
export const listDataQualityAnalyzers = /* GraphQL */ `
  query ListDataQualityAnalyzers(
    $filter: ModelDataQualityAnalyzerFilterInput
    $limit: Int
    $nextToken: String
  ) {
    listDataQualityAnalyzers(
      filter: $filter
      limit: $limit
      nextToken: $nextToken
    ) {
      items {
        id
        analyzerCode
        column
        enable
        tableHashKey
        createdAt
        updatedAt
      }
      nextToken
    }
  }
`;
export const suggestionByTable = /* GraphQL */ `
  query SuggestionByTable(
    $tableHashKey: String
    $sortDirection: ModelSortDirection
    $filter: ModelDataQualitySuggestionFilterInput
    $limit: Int
    $nextToken: String
  ) {
    suggestionByTable(
      tableHashKey: $tableHashKey
      sortDirection: $sortDirection
      filter: $filter
      limit: $limit
      nextToken: $nextToken
    ) {
      items {
        id
        constraint
        constraintCode
        column
        enable
        tableHashKey
        createdAt
        updatedAt
      }
      nextToken
    }
  }
`;
export const analyzerByTable = /* GraphQL */ `
  query AnalyzerByTable(
    $tableHashKey: String
    $sortDirection: ModelSortDirection
    $filter: ModelDataQualityAnalyzerFilterInput
    $limit: Int
    $nextToken: String
  ) {
    analyzerByTable(
      tableHashKey: $tableHashKey
      sortDirection: $sortDirection
      filter: $filter
      limit: $limit
      nextToken: $nextToken
    ) {
      items {
        id
        analyzerCode
        column
        enable
        tableHashKey
        createdAt
        updatedAt
      }
      nextToken
    }
  }
`;

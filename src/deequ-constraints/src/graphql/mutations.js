/* eslint-disable */
// this is an auto generated file. This will be overwritten

export const createDataQualitySuggestion = /* GraphQL */ `
  mutation CreateDataQualitySuggestion(
    $input: CreateDataQualitySuggestionInput!
    $condition: ModelDataQualitySuggestionConditionInput
  ) {
    createDataQualitySuggestion(input: $input, condition: $condition) {
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
export const updateDataQualitySuggestion = /* GraphQL */ `
  mutation UpdateDataQualitySuggestion(
    $input: UpdateDataQualitySuggestionInput!
    $condition: ModelDataQualitySuggestionConditionInput
  ) {
    updateDataQualitySuggestion(input: $input, condition: $condition) {
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
export const deleteDataQualitySuggestion = /* GraphQL */ `
  mutation DeleteDataQualitySuggestion(
    $input: DeleteDataQualitySuggestionInput!
    $condition: ModelDataQualitySuggestionConditionInput
  ) {
    deleteDataQualitySuggestion(input: $input, condition: $condition) {
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
export const createDataQualityAnalyzer = /* GraphQL */ `
  mutation CreateDataQualityAnalyzer(
    $input: CreateDataQualityAnalyzerInput!
    $condition: ModelDataQualityAnalyzerConditionInput
  ) {
    createDataQualityAnalyzer(input: $input, condition: $condition) {
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
export const updateDataQualityAnalyzer = /* GraphQL */ `
  mutation UpdateDataQualityAnalyzer(
    $input: UpdateDataQualityAnalyzerInput!
    $condition: ModelDataQualityAnalyzerConditionInput
  ) {
    updateDataQualityAnalyzer(input: $input, condition: $condition) {
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
export const deleteDataQualityAnalyzer = /* GraphQL */ `
  mutation DeleteDataQualityAnalyzer(
    $input: DeleteDataQualityAnalyzerInput!
    $condition: ModelDataQualityAnalyzerConditionInput
  ) {
    deleteDataQualityAnalyzer(input: $input, condition: $condition) {
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

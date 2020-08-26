import React, { useState, useEffect } from "react";
import { API, graphqlOperation } from "aws-amplify";
import styled from "@emotion/styled";
import { Button } from "@rebass/emotion";

import Analyzer from "./Analyzer";
import { listDataQualityAnalyzers } from "../graphql/queries";
import { createDataQualityAnalyzer as createDataQualityAnalyzerMutation, updateDataQualityAnalyzer, deleteDataQualityAnalyzer } from "../graphql/mutations";

const Container = styled("div")`
  max-width: 800px;
  margin: 16px auto;
  width: 100%;
`;

const Title = styled("h1")`
  margin-top: 0;
  margin-bottom: 0;
  color: #FF9900;
  font-size: 24px;
`;

const Label = styled("label")`
  color: #FF9900;
  display: inline-block;
  width: 140px;
  text-align: left;
`;

const RadioLabel = styled("label")`
  color: #FF9900;
  padding-left: 5px;
  padding-right: 2px;
`;

const Divider = styled("div")`
  height: 20px;
  background-color: #232f3e;
`;

const SubmitButton = styled(Button)`
  background-color: #FF9900;
  cursor: pointer;
`;

const checkProperties = (obj) => {
  for (var key in obj) {
    if (obj[key] === null || obj[key] === '')
        return false;
  }
  return true;
};

const initialFormState = { column: '', analyzerCode: '', enable: 'Y', database: '', table: '' }

export default () => {
  const [dataQualityAnalyzers, setDataQualityAnalyzers] = useState([]);
  const [filteredName, setFilteredName] = useState('')
  const [formData, setFormData] = useState(initialFormState);

  useEffect(() => {
    const fetchDataQualityAnalyzers = async () => {
      const result = await API.graphql(graphqlOperation(listDataQualityAnalyzers));
      setDataQualityAnalyzers(
        result.data.listDataQualityAnalyzers.items.sort((a, b) => {
          if (a.updatedAt > b.updatedAt) return -1;
          else return 1;
        })
      );
    };

    fetchDataQualityAnalyzers();
  }, []);

  async function createDataQualityAnalyzer() {
    if (!checkProperties(formData)) {
      window.alert(`All fields are required`)
      return
    }
    formData['tableHashKey'] = formData['database'].concat('-', formData['table']);
    const { ['database']: db, ['table']: tb, ...newInput } = formData;
    const apiData = await API.graphql({ query: createDataQualityAnalyzerMutation, variables: { input: newInput } });
    setDataQualityAnalyzers([ ...dataQualityAnalyzers, apiData.data.createDataQualityAnalyzer ]);
    setFormData(initialFormState);
  }

  // exclude column list from filter
  const excludeColumns = ["id"];
  // filter records by search text
  const filterData = (value) => {
    const lowercasedValue = value.toLowerCase().trim();
    if (lowercasedValue === '') return dataQualityAnalyzers;
    else {
      const filteredData = dataQualityAnalyzers.filter(item => {
        return Object.keys(item).some(key =>
          excludeColumns.includes(key) ? false : item[key].toString().toLowerCase().includes(lowercasedValue)
        );
      });
      return filteredData;
    }
  }

  const dataQualityAnalyzersToShow = filterData(filteredName)

  const handleFilteredChange = (event) => {
    setFilteredName(event.target.value)
  }

  return (
    <Container> 
      <Title>Filter:</Title> <input placeholder="E.g. mydb-table1 OR my_column" size="50" value={filteredName} onChange={handleFilteredChange}/>
      <Divider />
      <Title>Add a new analyzer:</Title>
          <div> <Label>Column:</Label> <input
            onChange={e => setFormData({ ...formData, 'column': e.target.value})}
            placeholder="Table Column"
            value={formData.column} size="50"
          /> </div>
          <div> <Label>Constraint Code:</Label> <input
            onChange={e => setFormData({ ...formData, 'analyzerCode': e.target.value})}
            placeholder="Constraint Code"
            value={formData.analyzerCode} size="50"
          /> </div>
          <div> <Label>Database:</Label> <input
            onChange={e => setFormData({ ...formData, 'database': e.target.value})}
            placeholder="Glue Database"
            value={formData.database} size="50"
          /> </div>
          <div> <Label>Table:</Label> <input
            onChange={e => setFormData({ ...formData, 'table': e.target.value})}
            placeholder="Glue Table"
            value={formData.table} size="50"
          /> </div>
          <div> <Label>Enable:</Label>
          <RadioLabel>Y</RadioLabel>
          <input
            type="radio" name="enable" value="Y" checked={true}
            onChange={e => setFormData({ ...formData, 'enable': e.target.value})}
          />
          <RadioLabel>N</RadioLabel>
          <input
            type="radio" name="enable" value="N"
            onChange={e => setFormData({ ...formData, 'enable': e.target.value})}
          /> </div>
          <div> <SubmitButton onClick={createDataQualityAnalyzer}>Create analyzer</SubmitButton> </div>
      <Divider />
      {dataQualityAnalyzersToShow.map(dataQualityAnalyzer => (
        <Analyzer
          key={dataQualityAnalyzer.id}
          {...dataQualityAnalyzer}
          toggleEnable={async () => {
            var enable = 'Y';
            if (dataQualityAnalyzer.enable === 'Y') enable='N';
            const changedAnalyzer = {...dataQualityAnalyzer, 'enable': enable};
            const {['createdAt']: c, ['updatedAt']: u, ...inputData} = changedAnalyzer;
            const result = await API.graphql(
              graphqlOperation(updateDataQualityAnalyzer, {
                input: {
                  ...inputData
                }
              })
            );

            setDataQualityAnalyzers(
              dataQualityAnalyzersToShow.map(n => {
                return n.id === dataQualityAnalyzer.id ? result.data.updateDataQualityAnalyzer : n;
              })
            );
          }}
          onDelete={async () => {
            await API.graphql(
              graphqlOperation(deleteDataQualityAnalyzer, {
                input: {
                  id: dataQualityAnalyzer.id
                }
              })
            );

            setDataQualityAnalyzers(dataQualityAnalyzersToShow.filter(n => n.id !== dataQualityAnalyzer.id));
          }}
        />
      ))}
    </Container>
  );
};
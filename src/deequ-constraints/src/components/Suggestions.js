import React, { useState, useEffect } from "react";
import { API, graphqlOperation } from "aws-amplify";
import styled from "@emotion/styled";
import { Button } from "@rebass/emotion";

import Suggestion from "./Suggestion";
import { listDataQualitySuggestions } from "../graphql/queries";
import { createDataQualitySuggestion as createDataQualitySuggestionMutation, updateDataQualitySuggestion, deleteDataQualitySuggestion } from "../graphql/mutations";

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

const initialFormState = { column: '', constraint: '', constraintCode: '', enable: 'Y', database: '', table: '' }

export default () => {
  const [dataQualitySuggestions, setDataQualitySuggestions] = useState([]);
  const [filteredName, setFilteredName] = useState('')
  const [formData, setFormData] = useState(initialFormState);

  useEffect(() => {
    const fetchDataQualitySuggestions = async () => {
      const result = await API.graphql(graphqlOperation(listDataQualitySuggestions));
      setDataQualitySuggestions(
        result.data.listDataQualitySuggestions.items.sort((a, b) => {
          if (a.updatedAt > b.updatedAt) return -1;
          else return 1;
        })
      );
    };

    fetchDataQualitySuggestions();
  }, []);

  async function createDataQualitySuggestion() {
    if (!checkProperties(formData)) {
      window.alert(`All fields are required`)
      return
    }
    formData['tableHashKey'] = formData['database'].concat('-', formData['table']);
    const { ['database']: db, ['table']: tb, ...newInput } = formData;
    const apiData = await API.graphql({ query: createDataQualitySuggestionMutation, variables: { input: newInput } });
    setDataQualitySuggestions([ ...dataQualitySuggestions, apiData.data.createDataQualitySuggestion ]);
    setFormData(initialFormState);
  }

  // exclude column list from filter
  const excludeColumns = ["id"];
  // filter records by search text
  const filterData = (value) => {
    const lowercasedValue = value.toLowerCase().trim();
    if (lowercasedValue === '') return dataQualitySuggestions;
    else {
      const filteredData = dataQualitySuggestions.filter(item => {
        return Object.keys(item).some(key =>
          excludeColumns.includes(key) ? false : item[key].toString().toLowerCase().includes(lowercasedValue)
        );
      });
      return filteredData;
    }
  }

  const dataQualitySuggestionsToShow = filterData(filteredName)

  const handleFilteredChange = (event) => {
    setFilteredName(event.target.value)
  }

  return (
    <Container> 
      <Title>Filter:</Title> <input placeholder="E.g. mydb-table1 OR my_column" size="50" value={filteredName} onChange={handleFilteredChange}/>
      <Divider />
      <Title>Add a new suggestion:</Title>
          <div> <Label>Column:</Label> <input
            onChange={e => setFormData({ ...formData, 'column': e.target.value})}
            placeholder="Table Column"
            value={formData.column} size="50"
          /> </div>
          <div> <Label>Constraint:</Label> <input
            onChange={e => setFormData({ ...formData, 'constraint': e.target.value})}
            placeholder="Constraint"
            value={formData.constraint} size="50"
          /> </div>
          <div> <Label>Constraint Code:</Label> <input
            onChange={e => setFormData({ ...formData, 'constraintCode': e.target.value})}
            placeholder="Constraint Code"
            value={formData.constraintCode} size="50"
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
          <div> <SubmitButton onClick={createDataQualitySuggestion}>Create suggestion</SubmitButton> </div>
      <Divider />
      {dataQualitySuggestionsToShow.map(dataQualitySuggestion => (
        <Suggestion
          key={dataQualitySuggestion.id}
          {...dataQualitySuggestion}
          toggleEnable={async () => {
            var enable = 'Y';
            if (dataQualitySuggestion.enable === 'Y') enable='N';
            const changedSuggestion = {...dataQualitySuggestion, 'enable': enable};
            const {['createdAt']: c, ['updatedAt']: u, ...inputData} = changedSuggestion;
            const result = await API.graphql(
              graphqlOperation(updateDataQualitySuggestion, {
                input: {
                  ...inputData
                }
              })
            );

            setDataQualitySuggestions(
              dataQualitySuggestionsToShow.map(n => {
                return n.id === dataQualitySuggestion.id ? result.data.updateDataQualitySuggestion : n;
              })
            );
          }}
          onDelete={async () => {
            await API.graphql(
              graphqlOperation(deleteDataQualitySuggestion, {
                input: {
                  id: dataQualitySuggestion.id
                }
              })
            );

            setDataQualitySuggestions(dataQualitySuggestionsToShow.filter(n => n.id !== dataQualitySuggestion.id));
          }}
        />
      ))}
    </Container>
  );
};
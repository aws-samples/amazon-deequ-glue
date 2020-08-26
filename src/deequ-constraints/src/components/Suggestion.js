import React from "react";
import styled from "@emotion/styled";
import { FaRegTrashAlt, FaRegCheckSquare } from "react-icons/fa";

const Suggestion = styled("div")`
  background-color: #FF9900;
  border-radius: 4px;
  margin-bottom: 24px;
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  align-items: stretch;
  overflow: hidden;
  box-shadow: 0 2px 4px rgba(116, 180, 155, 0.2);
`;

const LeftText = styled("p")`
  color: #232f3e;
  margin-top: 0;
  float: left;
`;

const RightText = styled("p")`
  color: #232f3e;
  margin-top: 0;
  float: right;
`;

const Icon = styled("button")`
  padding: 8px 10px;
  display: inline-flex;
  justify-content: center;
  align-items: center;
  color: #232f3e;
  border: none;
  cursor: pointer;
  flex: 1;
  background-color: #FF9900;

  &:hover {
    color: #FF9900;
    background-color: #232f3e;
  }
`;

const Divider = styled("div")`
  height: 2px;
  background-color: #f4f9f4;
`;

const SuggestionActions = styled("div")`
  display: flex;
  justify-content: stretch;
  align-items: stretch;
  height: 50px;
  background-color: #232f3e;
`;

const Info = styled.div`
  padding: 5px;
`;

export default props => {
  return (
    <Suggestion>
      <Info>
        <LeftText><b>Constraint:</b> {props.constraint}</LeftText>
        <RightText><b>Code:</b> {props.constraintCode}</RightText>
      </Info>
      <Info>
        <LeftText><b>Column:</b> {props.column}</LeftText>
        <RightText><b>Enabled:</b> {props.enable}</RightText>
      </Info>
      <Info>
        <LeftText><b>Database:</b> {props.tableHashKey.split('-')[0]}</LeftText>
        <RightText><b>Table:</b> {props.tableHashKey.split('-')[1]}</RightText>
      </Info>
      <Divider />
      <SuggestionActions>
        <Icon>
            <FaRegCheckSquare onClick={props.toggleEnable} />
        </Icon>
        <Icon>
          <FaRegTrashAlt onClick={props.onDelete} />
        </Icon>
      </SuggestionActions>
    </Suggestion>
  );
};
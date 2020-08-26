import React, { useState } from "react";
import styled from "@emotion/styled";
import { Auth } from "aws-amplify";
import { Button } from "@rebass/emotion";
import { Tabs, TabList, Tab, TabPanels, TabPanel } from "@reach/tabs";

import Suggestions from "./Suggestions";
import Analyzers from "./Analyzers";

const Header = styled("div")`
  background-color: #232f3e;
  padding-left: 16px;
  padding-right: 16px;
  display: flex;
  justify-content: space-between;
  align-items: center;
  position: fixed;
  right: 0;
  left: 0;
  height: 80px;
  z-index: 2;
`;

const Title = styled("h1")`
  margin-top: 0;
  margin-bottom: 0;
  text-transform: uppercase;
  color: #FF9900;
  font-size: 24px;
`;

const SignOutButton = styled(Button)`
  background-color: #FF9900;
  cursor: pointer;
`;

const StyledTabs = styled(Tabs)`
  padding-top: 80px;
  display: flex;
  flex-direction: column;
  height: calc(100vh - 80px);
`;

const StyledTabList = styled(TabList)`
  display: flex;
  justify-content: stretch;
  align-items: center;
  position: fixed;
  top: 80px;
  left: 0;
  right: 0;
  height: 40px;
  box-shadow: 0 6px 6px rgba(116, 180, 155, 0.4);

  & > [data-selected] {
    border-bottom-color: #FF9900;
    color: #FF9900;
  }
`;

const StyledTabPanels = styled(TabPanels)`
  padding-top: 50px;
  flex: 1;

  [hidden] {
    display: none;
  }
`;

const StyledTabPanel = styled(TabPanel)`
  flex: 1;
  padding: 16px;
  display: flex;
  min-height: calc(100% - 32px);
`;

const StyledTab = styled(Tab)`
  text-transform: uppercase;
  flex: 1;
  padding: 16px;
  color: #FF9900;
  background-color: #232f3e;
  font-size: 16px;
  border: none;
  border-bottom: 3px solid #232f3e;
`;

export default () => {
  const [tabIndex, setTabIndex] = useState(0);

  return (
    <>
      <Header>
        <Title>Deequ - Data Quality Constraints</Title>
        <SignOutButton
          onClick={() => {
            Auth.signOut().then(() => window.location.reload());
          }}
        >
          Sign Out
        </SignOutButton>
      </Header>
      <StyledTabs index={tabIndex} onChange={index => setTabIndex(index)}>
        <StyledTabList>
          <StyledTab>Suggestions</StyledTab>
          <StyledTab>Analyzers</StyledTab>
        </StyledTabList>
        <StyledTabPanels>
          <StyledTabPanel>
            {tabIndex === 0 && <Suggestions setTabIndex={setTabIndex}/>}
          </StyledTabPanel>
          <StyledTabPanel>
            {tabIndex === 1 && <Analyzers setTabIndex={setTabIndex}/>}
          </StyledTabPanel>
        </StyledTabPanels>
      </StyledTabs>
    </>
  );
};
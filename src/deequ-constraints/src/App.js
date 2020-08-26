import React, { useEffect, useState } from "react";
import { Auth } from "aws-amplify";
import { Authenticator } from "aws-amplify-react";
import styled from "@emotion/styled";

import awsExports from "./aws-exports";
import Screens from "./components/Screens";

const Title = styled("h1")`
  text-align: center;
  text-transform: uppercase;
  color: #FF9900;
  margin-bottom: 8px;
`;

const theme = {
  formContainer: {
    margin: 0,
    padding: "8px 24px 24px"
  },
  formSection: {
    backgroundColor: "#232f3e",
    borderRadius: "4px"
  },
  sectionHeader: {
    color: "#FF9900"
  },
  sectionFooterSecondaryContent: {
    color: "#303952"
  },
  inputLabel: {
    color: "#FF9900"
  },
  input: {
    backgroundColor: "#f4f9f4",
    color: "#FF9900"
  },
  hint: {
    color: "#FF9900"
  },
  button: {
    borderRadius: "3px",
    backgroundColor: "#FF9900"
  },
  a: {
    color: "#FF9900"
  }
};

function App() {
  const [state, setState] = useState({ isLoggedIn: false, user: null });

  const checkLoggedIn = () => {
    Auth.currentAuthenticatedUser()
      .then(data => {
        const user = { username: data.username, ...data.attributes };
        setState({ isLoggedIn: true, user });
      })
      .catch(error => console.log(error));
  };

  useEffect(() => {
    checkLoggedIn();
  }, []);

  return state.isLoggedIn ? (
    <Screens />
  ) : (
    <>
      <Title>Deequ - Data Quality Constraints</Title>
      <Authenticator
        onStateChange={authState => {
          if (authState === "signedIn") {
            checkLoggedIn();
          }
        }}
        amplifyConfig={awsExports}
        theme={theme}
      />
    </>
  );
}

export default App;
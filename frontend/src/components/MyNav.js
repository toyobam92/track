import Navbar from 'react-bootstrap/Navbar';
import Container from 'react-bootstrap/Container';
import Nav from 'react-bootstrap/Nav'
import ThemeToggler from './ThemeSwitcher';
import React, { useState } from 'react';

function MyNavBar() {
  const [isDark, setDark] = useState(true);
  return (
    <>
    {console.log(isDark)}
    <Navbar bg={isDark? "dark":"light"} variant={isDark? "dark":"light"}>
        <Container>
          <Navbar.Brand href="/"><h1>Track.io</h1></Navbar.Brand>
          <Nav className="me-auto">
            <Nav.Link href="/home">Home</Nav.Link>
            <Nav.Link href="/datasets">Datasets</Nav.Link>
            <Nav.Link href="/pricing">Pricing</Nav.Link>
          </Nav>
          <Nav>
          <ThemeToggler isDark={isDark} setDark={setDark}/>
        </Nav>
        </Container>
      </Navbar>

    </>
  );
}

export default MyNavBar;
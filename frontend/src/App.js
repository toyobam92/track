import './App.css';
import React from 'react';
import Home from "./Pages/Home"
import Datasets from "./components/Datasets";
import  {Route, Routes} from 'react-router-dom';

const App = () => {
  return (
    <Routes>
      <Route exact path='/' element=<Home/> />
      <Route exact path='/datasets' element=<Datasets/> />

    </Routes>
  );
}

export default App;

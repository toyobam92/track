import Accordion from 'react-bootstrap/Accordion';
import MyNavBar from "../components/MyNav";
import React, { useState } from 'react';

function Datasets() {
    const [datasets, setDatasets] = useState([{"name": "Combined Dataset"}, {"name": "Trained Dataset"}]);

  return (
    <>
    <MyNavBar/>
    <Accordion defaultActiveKey="1" className="dark">
        {
            datasets.map((dataset, index) => {
                return(
                    <Accordion.Item eventKey={`${index}`} className="dark" key={`${index}`}>
                    <Accordion.Header className="dark">
                        {dataset.name}
                    </Accordion.Header>
                    <Accordion.Body className="dark">
                        Temp
                    </Accordion.Body>
                  </Accordion.Item>
                )
            })

        }
    </Accordion>
    </>
  );
}

export default Datasets;
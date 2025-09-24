// import './DataPreview.css';

// import React, { useState, useEffect } from 'react';
// import { CircleLoader } from 'react-spinners'; // Changed from ScaleLoader to CircleLoader

// const DataPreview = () => {
//   const [cardsData, setCardsData] = useState([
//     { title: 'Total Tables', value: 'Loading...', trend: '' },
//     { title: 'Total Columns', value: 'Loading...', trend: '' },
//     { title: 'Total Rows', value: 'Loading...', trend: '' },
//   ]);
//   const [tables, setTables] = useState([]);
//   const [tableData, setTableData] = useState({});
//   const [openAccordion, setOpenAccordion] = useState(null);
//   const [loadingTables, setLoadingTables] = useState(true);
//   const [loadingTableData, setLoadingTableData] = useState({});

//   useEffect(() => {
//     setLoadingTables(true);
//     fetch('/api/tables')
//       .then(response => response.json())
//       .then(data => {
//         setTables(data.tables);
//         setCardsData([
//           { title: 'Total Tables', value: data.num_tables, trend: '' },
//           { title: 'Total Columns', value: data.total_columns, trend: '' },
//           { title: 'Total Rows', value: data.total_rows, trend: '' },
//         ]);
//         setLoadingTables(false);
//       })
//       .catch(error => {
//         console.error("Error fetching tables:", error);
//         setTables([]);
//         setLoadingTables(false);
//       });
//   }, []);

//   const toggleAccordion = async (tableName) => {
//     if (openAccordion === tableName) {
//       setOpenAccordion(null);
//       return;
//     }

//     setOpenAccordion(tableName);

//     setLoadingTableData(prev => ({ ...prev, [tableName]: true }));
//     try {
//       const response = await fetch(`/api/table_data?table_name=${tableName}`);
//       const data = await response.json();
//       setTableData(prevData => {
//         const newTableData = { ...prevData };
//         newTableData[tableName] = data;
//         return newTableData;
//       });
//     } catch (error) {
//       console.error("Error fetching table data:", error);
//       setTableData(prevData => ({
//         ...prevData,
//         [tableName]: { error: "Failed to load data" }
//       }));
//     } finally {
//       setLoadingTableData(prev => ({ ...prev, [tableName]: false }));
//     }
//   };

//   return (
//     <div className="data-preview-page">
//       <header className="page-header">
//         <h1>BigQuery Dataset Preview</h1>
//       </header>
//       <div className="cards-container">
//         {cardsData.map((card, index) => (
//           <div key={index} className="data-card">
//             <h3>{card.title}</h3>
//             <p className="card-value">
//               {card.value === 'Loading...' ? <CircleLoader color="#36D7B7" size={20} /> : card.value}
//             </p>
//             <p className="card-trend">{card.trend}</p>
//           </div>
//         ))}
//       </div>

//       <div className="accordion-container">
//         <h2>BigQuery Dataset</h2>
//         {loadingTables ? (
//           <div className="spinner-container">
//             <CircleLoader color="#36D7B7" size={30} /> {/* Changed to CircleLoader, added size prop */}
//           </div>
//         ) : (
//           tables.map((tableName) => (
//             <div key={tableName} className={`accordion-item ${openAccordion === tableName ? 'open' : ''}`}>
//               <button
//                 className="accordion-header"
//                 onClick={() => toggleAccordion(tableName)}
//               >
//                 <span>{tableName}</span>
//                 <span>{openAccordion === tableName ? '-' : '+'}</span>
//               </button>
//               {openAccordion === tableName && (
//                 <div className="accordion-content">
//                   {loadingTableData[tableName] ? (
//                     <div className="spinner-container">
//                       <CircleLoader color="#36D7B7" size={20} /> {/* Changed to CircleLoader, added size prop */}
//                     </div>
//                   ) : tableData[tableName] ? (
//                     <>
//                       <div className="metadata-section">
//                         <h4>Metadata:</h4>
//                         {tableData[tableName].error ? (
//                           <p>Error loading metadata.</p>
//                         ) : (
//                           <>
//                             <p><strong>Description:</strong> {tableData[tableName].description}</p>
//                           </>
//                         )}
//                       </div>
//                       <h4>Table Data:</h4>
//                       <div className="table-responsive">
//                         <table>
//                           <thead>
//                             <tr>
//                               {tableData[tableName].data && tableData[tableName].data.length > 0 &&
//                                 Object.keys(tableData[tableName].data[0]).map(col => (
//                                   <th key={col}>{col}</th>
//                                 ))}
//                             </tr>
//                           </thead>
//                           <tbody>
//                             {tableData[tableName].data && tableData[tableName].data.map((row, rowIndex) => (
//                               <tr key={rowIndex}>
//                                 {Object.values(row).map((value, colIndex) => (
//                                   <td key={colIndex}>{String(value)}</td>
//                                 ))}
//                               </tr>
//                             ))}
//                           </tbody>
//                         </table>
//                       </div>
//                     </>
//                   ) : (
//                     <p>Loading data...</p>
//                   )}
//                 </div>
//               )}
//             </div>
//           ))
//         )}
//       </div>
//     </div>
//   );
// };

// export default DataPreview;
import './DataPreview.css';
import React, { useState, useEffect } from 'react';
import { CircleLoader } from 'react-spinners'; // Changed from ScaleLoader to CircleLoader

const DataPreview = () => {
  const [cardsData, setCardsData] = useState([
    { title: 'Total Tables', value: 'Loading...', trend: '' },
    { title: 'Total Columns', value: 'Loading...', trend: '' },
    { title: 'Total Rows', value: 'Loading...', trend: '' },
  ]);
  const [tables, setTables] = useState([]);
  const [tableData, setTableData] = useState({});
  const [openAccordion, setOpenAccordion] = useState(null);
  const [loadingTables, setLoadingTables] = useState(true);
  const [loadingTableData, setLoadingTableData] = useState({});

  useEffect(() => {
    setLoadingTables(true);
    fetch('/api/tables')
      .then(response => response.json())
      .then(data => {
        setTables(data.tables || []);
        setCardsData([
          { title: 'Total Tables', value: data.num_tables, trend: '' },
          { title: 'Total Columns', value: data.total_columns, trend: '' },
          { title: 'Total Rows', value: data.total_rows, trend: '' },
        ]);
        setLoadingTables(false);
      })
      .catch(error => {
        console.error("Error fetching tables:", error);
        setTables([]);
        setLoadingTables(false);
      });
  }, []);

  const toggleAccordion = async (tableName) => {
    if (openAccordion === tableName) {
      setOpenAccordion(null);
      return;
    }

    setOpenAccordion(tableName);
    setLoadingTableData(prev => ({ ...prev, [tableName]: true }));

    try {
      const response = await fetch(`/api/table_data?table_name=${tableName}`);
      const data = await response.json();
      setTableData(prevData => ({
        ...prevData,
        [tableName]: data
      }));
    } catch (error) {
      console.error("Error fetching table data:", error);
      setTableData(prevData => ({
        ...prevData,
        [tableName]: { error: "Failed to load data" }
      }));
    } finally {
      setLoadingTableData(prev => ({ ...prev, [tableName]: false }));
    }
  };

  return (
    <div className="data-preview-page">
      <header className="page-header">
        <h1>BigQuery Dataset Preview</h1>
      </header>

      {/* Cards Section */}
      <div className="cards-container">
        {cardsData.map((card, index) => (
          <div key={index} className="data-card">
            <h3>{card.title}</h3>
            <p className="card-value">
              {card.value === 'Loading...' ? (
                <CircleLoader color="#36D7B7" size={20} />
              ) : (
                card.value
              )}
            </p>
            <p className="card-trend">{card.trend}</p>
          </div>
        ))}
      </div>

      {/* Accordion Section */}
      <div className="accordion-container">
        <h2>BigQuery Dataset</h2>
        {loadingTables ? (
          <div className="spinner-container">
            <CircleLoader color="#36D7B7" size={30} />
          </div>
        ) : (
          tables.map((tableName) => (
            <div
              key={tableName}
              className={`accordion-item ${openAccordion === tableName ? 'open' : ''}`}
            >
              <button
                className="accordion-header"
                onClick={() => toggleAccordion(tableName)}
              >
                <span>{tableName}</span>
                <span>{openAccordion === tableName ? '-' : '+'}</span>
              </button>

              {openAccordion === tableName && (
                <div className="accordion-content">
                  {loadingTableData[tableName] ? (
                    <div className="spinner-container">
                      <CircleLoader color="#36D7B7" size={20} />
                    </div>
                  ) : tableData[tableName] ? (
                    <>
                      <div className="metadata-section">
                        <h4>Metadata:</h4>
                        {tableData[tableName].error ? (
                          <p>Error loading metadata.</p>
                        ) : (
                          <p><strong>Description:</strong> {tableData[tableName].description}</p>
                        )}
                      </div>

                      <h4>Table Data:</h4>
                      <div className="table-responsive">
                        <table>
                          <thead>
                            <tr>
                              {tableData[tableName]?.data?.length > 0 &&
                                Object.keys(tableData[tableName].data[0]).map((col) => (
                                  <th key={col}>{col}</th>
                                ))}
                            </tr>
                          </thead>
                          <tbody>
                            {tableData[tableName]?.data?.length > 0 ? (
                              tableData[tableName].data.map((row, rowIndex) => (
                                <tr key={rowIndex}>
                                  {Object.values(row).map((value, colIndex) => (
                                    <td key={colIndex}>{String(value)}</td>
                                  ))}
                                </tr>
                              ))
                            ) : (
                              <tr>
                                <td colSpan="100%">No data available</td>
                              </tr>
                            )}
                          </tbody>
                        </table>
                      </div>
                    </>
                  ) : (
                    <p>Loading data...</p>
                  )}
                </div>
              )}
            </div>
          ))
        )}
      </div>
    </div>
  );
};

export default DataPreview;
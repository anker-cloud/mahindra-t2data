import React, { useState, useEffect } from 'react';
import Highlight from 'react-highlight'; // Import Highlight component
// No need to import styles directly in JS with react-highlight
import './SolutionArchitecture.css';
// Import a highlight.js theme in SolutionArchitecture.css
import archiDiagram from '../../assets/archi.png';

const SolutionArchitecture = () => {
  const [agentCode, setAgentCode] = useState('');
  const [instructionsCode, setInstructionsCode] = useState('');
  const [customToolsCode, setCustomToolsCode] = useState('');
  const [instructionsYamlCode, setInstructionsYamlCode] = useState(''); // New state for YAML
  const [loadingCode, setLoadingCode] = useState(true);
  const [errorLoadingCode, setErrorLoadingCode] = useState(false);

  // State for collapsible sections
  const [isInstructionsYamlCodeCollapsed, setIsInstructionsYamlCodeCollapsed] = useState(false); // New state for YAML, default open
  const [isAgentCodeCollapsed, setIsAgentCodeCollapsed] = useState(true);
  const [isInstructionsCodeCollapsed, setIsInstructionsCodeCollapsed] = useState(true);
  const [isCustomToolsCodeCollapsed, setIsCustomToolsCodeCollapsed] = useState(true);

  const filesToFetch = [
    { name: 'data_agent/instructions.yaml', setter: setInstructionsYamlCode }, // Add YAML first
    { name: 'data_agent/agent.py', setter: setAgentCode },
    { name: 'data_agent/instructions.py', setter: setInstructionsCode },
    { name: 'data_agent/custom_tools.py', setter: setCustomToolsCode },
  ];

  useEffect(() => {
    const fetchCode = async () => {
      setLoadingCode(true);
      setErrorLoadingCode(false);
      try {
        for (const file of filesToFetch) {
          const response = await fetch(`/api/code?filepath=${file.name}`);
          if (!response.ok) {
            throw new Error(`Failed to fetch code for ${file.name}: ${response.status}`);
          }
          const data = await response.json();
          file.setter(data.content);
        }
        setLoadingCode(false);
      } catch (error) {
        console.error("Error fetching code files:", error);
        setErrorLoadingCode(true);
        setLoadingCode(false);
      }
    };

    fetchCode();
  }, []); // Empty dependency array means this effect runs once on mount

  return (
    <div className="solution-architecture-page">
      <header className="page-header">
        <h1>Solution Architecture</h1>
      </header>
      <div className="architecture-diagram-container">
        {/* Display the architecture diagram image */}
        <img src={archiDiagram} alt="Solution Architecture Diagram" className="architecture-diagram" />
      </div>

      <div className="code-section-container">
        <h2>Key Code Files</h2>
        {loadingCode ? (
          <p>Loading code...</p>
        ) : errorLoadingCode ? (
          <p className="error-message">Failed to load code files.</p>
        ) : (
          <div className="code-blocks-rows"> {/* Changed class name */}
            {/* Instructions YAML Section (New) */}
            <div className="code-block-row">
              <h3 onClick={() => setIsInstructionsYamlCodeCollapsed(!isInstructionsYamlCodeCollapsed)} className="collapsible-header">
                data_agent/instructions.yaml {isInstructionsYamlCodeCollapsed ? '▼' : '▲'}
              </h3>
              {!isInstructionsYamlCodeCollapsed && (
                <div className="code-content">
                  <Highlight language="yaml">
                    {instructionsYamlCode}
                  </Highlight>
                </div>
              )}
            </div>

            {/* Agent Code Section */}
            <div className="code-block-row"> {/* Changed class name */}
              <h3 onClick={() => setIsAgentCodeCollapsed(!isAgentCodeCollapsed)} className="collapsible-header">
                data_agent/agent.py {isAgentCodeCollapsed ? '▼' : '▲'}
              </h3>
              {!isAgentCodeCollapsed && (
                <div className="code-content">
                  <Highlight language="python">
                    {agentCode}
                  </Highlight>
                </div>
              )}
            </div>

            {/* Instructions Code Section */}
            <div className="code-block-row"> {/* Changed class name */}
              <h3 onClick={() => setIsInstructionsCodeCollapsed(!isInstructionsCodeCollapsed)} className="collapsible-header">
                data_agent/instructions.py {isInstructionsCodeCollapsed ? '▼' : '▲'}
              </h3>
              {!isInstructionsCodeCollapsed && (
                <div className="code-content">
                  <Highlight language="python">
                    {instructionsCode}
                  </Highlight>
                </div>
              )}
            </div>

            {/* Custom Tools Code Section */}
            <div className="code-block-row"> {/* Changed class name */}
              <h3 onClick={() => setIsCustomToolsCodeCollapsed(!isCustomToolsCodeCollapsed)} className="collapsible-header">
                data_agent/custom_tools.py {isCustomToolsCodeCollapsed ? '▼' : '▲'}
              </h3>
              {!isCustomToolsCodeCollapsed && (
                <div className="code-content">
                  <Highlight language="python">
                    {customToolsCode}
                  </Highlight>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </div>
  );
};

export default SolutionArchitecture;
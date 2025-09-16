// App.js
import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import './App.css'; // Ensure this global CSS is imported

// Import your components
import ChatInterface from './components/ChatInterface/ChatInterface'; // Assuming this is your page component
import VerticalNav from './components/VerticalNav/VerticalNav';
import DataPreview from './components/DataPreview/DataPreview'; // Assuming this is your page component
import SolutionArchitecture from './components/SolutionArchitecture/SolutionArchitecture'; // Assuming this is your page component
import HorizontalNav from './components/HorizontalNav/HorizontalNav';

/**
 * Main application component.
 * Sets up the overall layout with a horizontal navigation bar at the top,
 * a vertical navigation bar on the side, and the main content area.
 * Uses React Router for handling different page views.
 */
function App() {
  return (
    <Router>
      {/* Main container for the entire application page */}
      <div className="App-container">
        {/* Horizontal navigation bar at the top of the page */}
        <HorizontalNav />

        {/* Wrapper for the content area below the HorizontalNav, containing VerticalNav and Main Content */}
        <div className="App-content-area">
          {/* Vertical navigation panel on the left side */}
          <VerticalNav />

          {/* Main content area where routed pages will be displayed */}
          <main className="App-main-content">
            <Routes>
              {/* Default route, e.g., to ChatInterface */}
              <Route path="/" element={<ChatInterface />} />
              {/* Route to DataPreview page */}
              <Route path="/data-preview" element={<DataPreview />} />
              {/* Route to SolutionArchitecture page */}
              <Route path="/solution-architecture" element={<SolutionArchitecture />} />
              {/* It's good practice to also have a specific route for chat if it's not the only default */}
              <Route path="/chat" element={<ChatInterface />} />
            </Routes>
          </main>
        </div> {/* End of App-content-area */}
      </div> {/* End of App-container */}
    </Router>
  );
}

export default App;

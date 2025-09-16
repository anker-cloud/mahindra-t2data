import React, { useState, useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import './App.css';

import LoginPage from './components/LoginPage/LoginPage';
import ChatInterface from './components/ChatInterface/ChatInterface';
import VerticalNav from './components/VerticalNav/VerticalNav';
import DataPreview from './components/DataPreview/DataPreview';
import SolutionArchitecture from './components/SolutionArchitecture/SolutionArchitecture';
import HorizontalNav from './components/HorizontalNav/HorizontalNav';

function App() {
  const [username, setUsername] = useState(() => localStorage.getItem('username') || '');
  const [isLoading, setIsLoading] = useState(false);

  useEffect(() => {
    if (username) {
      localStorage.setItem('username', username);
    } else {
      localStorage.removeItem('username');
    }
  }, [username]);

  const handleLogin = (userData) => {
    setIsLoading(true);
    setUsername(userData.username);
    console.log('Logged in as:', userData.username, 'with role:', userData.role);
    setIsLoading(false);
  };

  const handleLogout = () => {
    setUsername('');
    console.log('Logged out');
  };

  if (!username) {
    return <LoginPage onLogin={handleLogin} />;
  }

  return (
    <Router>
      <div className="App-container">
        <HorizontalNav username={username} onLogout={handleLogout} />
        <div className="App-content-area">
          <VerticalNav />
          <main className="App-main-content">
            <Routes>
              <Route
                path="/"
                element={<ChatInterface username={username} />}
              />
              <Route
                path="/chat"
                element={<ChatInterface username={username} />}
              />
              <Route path="/data-preview" element={<DataPreview />} />
              <Route path="/solution-architecture" element={<SolutionArchitecture />} />
            </Routes>
          </main>
        </div>
      </div>
    </Router>
  );
}

export default App;
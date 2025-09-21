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
    // --- STATE MANAGEMENT ---
    // Now we store both username and sessionId
    const [username, setUsername] = useState(() => localStorage.getItem('username') || '');
    const [sessionId, setSessionId] = useState(() => localStorage.getItem('sessionId') || '');

    useEffect(() => {
        // Persist both username and sessionId to localStorage
        if (username && sessionId) {
            localStorage.setItem('username', username);
            localStorage.setItem('sessionId', sessionId);
        } else {
            localStorage.removeItem('username');
            localStorage.removeItem('sessionId');
        }
    }, [username, sessionId]);

    // --- LOGIN/LOGOUT HANDLERS ---
    const handleLogin = (loginData) => {
        // This now receives both username and sessionId from LoginPage
        setUsername(loginData.username);
        setSessionId(loginData.sessionId);
        console.log('Logged in as:', loginData.username, 'with session:', loginData.sessionId);
    };

    const handleLogout = () => {
        // Clear both username and sessionId on logout
        setUsername('');
        setSessionId('');
        console.log('Logged out');
    };

    // --- RENDER LOGIC ---
    // If we don't have a username/session, show the LoginPage
    if (!username || !sessionId) {
        return <LoginPage onLogin={handleLogin} />;
    }

    // If we are logged in, show the main application dashboard
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
                                // Pass both username and sessionId to the ChatInterface
                                element={<ChatInterface username={username} sessionId={sessionId} />}
                            />
                            <Route
                                path="/chat"
                                element={<ChatInterface username={username} sessionId={sessionId} />}
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
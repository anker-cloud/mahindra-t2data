import React, { useState } from 'react';
import './LoginPage.css';
import MahindraRiseRED from "../../assets/Mahindra Rise RED.png";

const LoginPage = ({ onLogin }) => {
    const [userId, setUserId] = useState('');
    const [password, setPassword] = useState('');
    const [role, setRole] = useState('services'); // Retained from your original layout
    const [error, setError] = useState('');

    const handleLogin = async (e) => {
        e.preventDefault();
        
        // 1. Hardcoded password for the POC is checked here.
        const POC_PASSWORD = "mahindra123";

        // 2. This is the check. If it fails, the function stops and shows an error.
        if (password !== POC_PASSWORD) {
            setError("Invalid password.");
            return; 
        }

        if (!userId.trim()) {
            setError("Username cannot be empty.");
            return;
        }

        setError(''); // Clear any previous errors before proceeding

        try {
            // 3. This part is only reached if the password is correct.
            const response = await fetch('/api/login', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ user_id: userId }),
            });

            if (!response.ok) {
                const errData = await response.json();
                throw new Error(errData.error || 'Failed to create session.');
            }

            const data = await response.json();
            
            // 4. On success, call the onLogin function from App.js with the new data.
            if (onLogin) {
                onLogin({ username: data.user_id, sessionId: data.session_id });
            }

        } catch (err) {
            setError(err.message || 'An error occurred during login. Please try again.');
        }
    };

    return (
        <div className="login-container">
            <form onSubmit={handleLogin} className="login-form">
                <img src={MahindraRiseRED} alt="Mahindra Rise RED" className="logo" />
                {/* <h3 className="welcome-text">Welcome to Talk to Data</h3> */}

                {error && <p className="error-message">{error}</p>}

                <div className="form-group">
                    <label htmlFor="userId">Username</label>
                    <input
                        type="text"
                        id="userId"
                        value={userId}
                        onChange={(e) => setUserId(e.target.value)}
                        placeholder="Enter Username"
                        autoComplete="username"
                        className="login-input"
                    />
                </div>
                <div className="form-group">
                    <label htmlFor="password">Password</label>
                    <input
                        type="password"
                        id="password"
                        value={password}
                        onChange={(e) => setPassword(e.target.value)}
                        placeholder="Enter Password"
                        autoComplete="current-password"
                        className="login-input"
                    />
                </div>
                <div className="form-group">
                    <label htmlFor="role">Roles</label>
                    <select
                        id="role"
                        value={role}
                        onChange={(e) => setRole(e.target.value)}
                        className="login-select"
                    >
                        <option value="services">Services</option>
                        <option value="admin">Admin</option>
                    </select>
                </div>
                
                <button type="submit" className="login-button">Login</button>
            </form>
        </div>
    );
};

export default LoginPage;
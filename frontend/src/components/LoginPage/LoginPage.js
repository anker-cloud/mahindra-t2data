import React, { useState } from "react";
import "./LoginPage.css";
import mahindraLogo from "../../assets/mahindra-logo.png";

const LoginPage = ({ onLogin }) => {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [role, setRole] = useState("services");

  // Hardcoded credentials
  const validUser = "admin";
  const validPass = "mahindra123";

  const handleSubmit = (e) => {
    e.preventDefault();

    if (!username.trim() || !password.trim()) {
      alert("Please enter both username and password");
      return;
    }

    if (username === validUser && password === validPass) {
      onLogin({ username: username.trim(), role });
    } else {
      alert("Invalid username or password");
    }
  };

  return (
    <div className="login-container">
      <div className="login-card">
        <img src={mahindraLogo} alt="Mahindra Logo" className="logo" />
        <h2 className="title">Welcome to Talk to Data</h2>

        <form onSubmit={handleSubmit} className="login-form">
          <label className="label">Username</label>
          <input
            className="input"
            type="text"
            placeholder="Enter Username"
            value={username}
            onChange={(e) => setUsername(e.target.value)}
          />

          <label className="label">Password</label>
          <input
            className="input"
            type="password"
            placeholder="Enter Password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
          />

          <label className="label">Roles</label>
          <select
            className="input"
            value={role}
            onChange={(e) => setRole(e.target.value)}
          >
            <option value="services">Services</option>
            <option value="sales">Sales</option>
          </select>

          <button type="submit" className="login-btn">Login</button>
        </form>
      </div>
    </div>
  );
};

export default LoginPage;
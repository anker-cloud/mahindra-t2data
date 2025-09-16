import React from 'react';
import './HorizontalNav.css';
import mahindraLogo from '../../assets/mahindra-logo.png'; // Replace with your Mahindra PNG logo

const HorizontalNav = ({ username, onLogout }) => {
  return (
    <nav className="horizontal-nav">
      <div className="nav-left">
        {/* Logo and App Title */}
        <div className="app-info">
          <img src={mahindraLogo} alt="Mahindra Logo" height="36" className="app-logo" />
          <span className="app-title">Talk to Data(POC)</span>
        </div>
      </div>

      <div className="nav-right">
        <span className="user-name">Welcome, {username} 👋</span>
        <button className="logout-btn" onClick={onLogout}>Logout</button>
      </div>
    </nav>
  );
};

export default HorizontalNav;
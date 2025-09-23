import React from 'react';
import './HorizontalNav.css';
import MDP_logo from '../../assets/MDP_logo.png'; // Replace with your Mahindra PNG logo

const HorizontalNav = ({ username, onLogout }) => {
  return (
    <nav className="horizontal-nav">
      <div className="nav-left">
        {/* Logo and App Title */}
        <div className="app-info">
          <img src={MDP_logo} alt="MDP_logo" height="36" className="app-logo" />
          <span className="app-title">DIA</span>
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
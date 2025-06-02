import React from 'react';
import './HorizontalNav.css';
import logo from '../../assets/logo.png'; // Import the logo image
import face from '../../assets/face.png'; // Import the face image

const HorizontalNav = () => {
  return (
    <nav className="horizontal-nav">
      <div className="nav-left">
        {/* Logo and App Title */}
        <div className="app-info">
          {/* Reference logo using imported variable */}
          <img src={logo} alt="Retail DataWise Agent Logo" height="32" className="app-logo" />
          <span className="app-title">Retail DataWise Agent</span>
        </div>
        {/* Navigation links can be added here later if needed */}
      </div>
      <div className="nav-right">
        {/* Dummy navigation links if needed */}
        {/* <ul className="nav-links">
          <li><a href="#home" className="nav-link">Home</a></li>
        </ul> */}
        {/* User options */}
        <div className="user-options">
          {/* Use face.png for user avatar */}
          <img src={face} alt="User Avatar" height="32" className="user-avatar" />
          <span className="user-name">Jane Doe</span> {/* Placeholder user name */}
        </div>
      </div>
    </nav>
  );
};

export default HorizontalNav;
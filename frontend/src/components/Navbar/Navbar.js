import React from 'react';
import './Navbar.css';
import logo from '../../assets/logo.png'; // Import the logo image

// Placeholder for a logo, you can replace this with an SVG or an <img> tag
const Logo = () => (
  <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" fill="currentColor" width="32" height="32">
    <path d="M12 15a3 3 0 100-6 3 3 0 000 6z"/>
    <path fillRule="evenodd" d="M1.866 5.5V3.866l1.85-.183A2.25 2.25 0 016 2.25h3.818a2.25 2.25 0 012.132 1.432l1.839 1.839c.17.17.32.351.456.542A2.25 2.25 0 0118 7.5V9.318a2.25 2.25 0 01-1.432 2.132l-1.839 1.839a2.25 2.25 0 00-.542.456 2.25 2.25 0 01-1.542 1.432L9.318 18H7.5a2.25 2.25 0 01-2.132-1.432l-1.839-1.839A2.25 2.25 0 003.373 14.15l-.183 1.851V18.134l1.85-.183a2.25 2.25 0 011.432 2.132L7.5 21.75h1.818a2.25 2.25 0 012.132-1.432l1.839-1.839.456-.542a2.25 2.25 0 011.432-1.542l1.851-.183V5.866l-.183 1.85A2.25 2.25 0 0118.134 6H20.25a.75.75 0 000-1.5h-2.116a2.25 2.25 0 01-2.132-1.432L14.15 1.866l-1.85.183A2.25 2.25 0 0110.5 2.25H8.682a2.25 2.25 0 01-2.132-1.432L4.711.373l-.183 1.85A2.25 2.25 0 003.866 3.866H1.866zM6.61 10.18c.38.05.75.11 1.116.185l.305-.914a1.5 1.5 0 011.407-.956h1.094c.49 0 .96.165 1.345.474l.254.203a1.5 1.5 0 001.183.186l.914-.305c.075.366.135.736.185 1.116l.914.305a1.5 1.5 0 01.956 1.407v1.094c0 .49-.165.96-.474 1.345l-.203.254a1.5 1.5 0 00-.186 1.183l.305.914c-.366.075-.736.135-1.116.185l-.914.305a1.5 1.5 0 01-1.407.956h-1.094c-.49 0-.96-.165-1.345-.474l-.254-.203a1.5 1.5 0 00-1.183-.186l-.914.305c-.075-.366-.135-.736-.185-1.116l-.914-.305a1.5 1.5 0 01-.956-1.407V12.5c0-.49.165-.96.474-1.345l.203-.254a1.5 1.5 0 00.186-1.183l-.305-.914z" clipRule="evenodd" />
  </svg>
);


const Navbar = () => {
  return (
    <nav className="navbar">
      <div className="navbar-container">
        <a href="/" className="navbar-logo">
          <Logo />
          <span className="navbar-title">Retail DataWise Agent</span>
        </a>
        {/* Navigation links can be added here later */}
        {/* <ul className="nav-menu">
          <li className="nav-item">
            <a href="#home" className="nav-link">Home</a>
          </li>
          <li className="nav-item">
            <a href="#about" className="nav-link">About</a>
          </li>
        </ul> */}
      </div>
      <div className="navbar-footer">Powered by APAC Solutions Acceleration Team</div>
    </nav>
  );
};

export default Navbar;
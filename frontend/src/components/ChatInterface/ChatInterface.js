import React, { useState, useEffect, useRef } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import './ChatInterface.css';

// Placeholder for an icon, you would typically use an SVG or an icon library
const SendIcon = () => (
  <svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
    <path d="M2.01 21L23 12L2.01 3L2 10L17 12L2 14L2.01 21Z" fill="currentColor"/>
  </svg>
);

// Simple Typing Indicator Component
const TypingIndicator = () => {
  const phrases = [
    "  Thinking about the query",
    "  Understanding the table schema and underlying data",
    "  Generating a plan to come up with answers and insights",
    "  Performing accurate NL2SQL conversion",
    "  Making API calls to BigQuery",
    "  Fetching SQL response from BigQuery",
    "  Processing result",
  ];
  const [currentPhraseIndex, setCurrentPhraseIndex] = useState(0);

  useEffect(() => {
    const interval = setInterval(() => {
      setCurrentPhraseIndex((prevIndex) => (prevIndex + 1) % phrases.length);
    }, 5000); // Change phrase every 2 seconds

    return () => clearInterval(interval); // Clean up the interval on component unmount
  }, [phrases.length]);

  return (
    <div className="message bot typing-indicator">
      <div className="message-bubble">
      <div className="typing-dots">
          <span className="dot"></span>
          <span className="dot"></span>
          <span className="dot"></span>
        </div>
        <div className="typing-text">
          {phrases[currentPhraseIndex]}
        </div>
      </div>
    </div>
  );
};

const suggestedQuestions = [
  { heading: 'Understanding BQ Dataset', question: 'Describe the tables and the data that you can answer questions over.' },
  { heading: 'Higher Distribution Cost', question: 'Identify and show distribution centers with unusually high distribution costs relative to the average distribution costs for similar product categories?'},
  { heading: 'Problematic Product categories', question: 'Calculate the return rate per product category, considering only orders that have been both shipped and delivered, to identify problematic product categories?' },
  { heading: 'Multi-Channel Attribution', question: 'Calculate the conversion rate (percentage of events leading to an order) for each browser type, to optimize website compatibility and user experience?' },
  { heading: 'Most Expensive Products', question: 'Find the top 5 most expensive products (based on retail price) within each category?' },
  { heading: 'Potential Bot Attacks', question: 'Identify IP addresses associated with a disproportionately high number of distinct user sessions, potentially indicating shared proxies or bot networks?' },
];

const ChatInterface = () => {
  const [messages, setMessages] = useState([
    { id: 1, text: "Hello! I am Retail DataWise. I can answer business questions over your BigQuery data. How can I assist you today?", sender: 'bot', timestamp: new Date() }
  ]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [sessionId, setSessionId] = useState(null);
  const [userId] = useState(`user_${Date.now()}`); // Simple unique user ID

  const messagesEndRef = useRef(null);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(scrollToBottom, [messages]);

  const handleInputChange = (e) => {
    setInputValue(e.target.value);
  };

  const handleSuggestionClick = (question) => {
    setInputValue(question); // Set the input value to the suggested question
    // Programmatically trigger the send message action
    // We need to simulate the event object or modify handleSendMessage to accept the message directly
    // Let's modify handleSendMessage to accept an optional message string
    // handleSendMessage({ preventDefault: () => {} }, question); // Pass a dummy event and the question
  };

  // Modify handleSendMessage to accept an optional message string
  const handleSendMessage = async (e, messageString = inputValue) => {
    e.preventDefault();
    const messageToSend = messageString.trim(); // Use the passed string or current inputValue
    if (messageToSend === '' || isLoading) return;

    const userMessage = { id: Date.now(), text: messageToSend, sender: 'user', timestamp: new Date() };
    setMessages(prevMessages => [...prevMessages, userMessage]);
    setInputValue(''); // Clear input after sending
    setIsLoading(true);

    try {
      let requestBody;
      const currentInputValue = messageToSend; // Use the captured messageToSend

      if (!sessionId) {
        console.log("No session ID, creating a new session and sending message...");
        requestBody = {
          user_id: userId,
          message: { message: currentInputValue, role: 'user' }
        };
      } else {
        console.log("Existing session ID, sending message:", sessionId);
        requestBody = {
          user_id: userId,
          session_id: sessionId,
          message: { message: currentInputValue, role: 'user' }
        };
      }

      const response = await fetch('/api/chat', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(requestBody)
      });

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ detail: "Unknown error occurred" }));
        throw new Error(`HTTP error! status: ${response.status} - ${errorData.detail || errorData.error || "Failed to send/process message"}`);
      }

      const data = await response.json();

      if (!sessionId && data.session_id) {
        setSessionId(data.session_id);
        console.log("New session created and ID set:", data.session_id);
      }

      if (data.messages && data.messages.length > 0) {
        const botReplies = data.messages.map((msg, index) => ({
          id: Date.now() + index,
          text: msg.content,
          sender: msg.role === 'user' ? 'user' : 'bot',
          timestamp: new Date()
        }));
        setMessages(prevMessages => [...prevMessages, ...botReplies]);
      } else if (data.error) {
         throw new Error(data.error);
      } else if (!sessionId && !data.session_id) {
        throw new Error("Failed to retrieve session ID and no messages received.");
      } else if (data.messages && data.messages.length === 0) {
        console.warn("Received empty messages array from bot, but no error:", data);
      }

    } catch (error) {
      console.error('Error sending message:', error);
      setMessages(prevMessages => [...prevMessages, { id: Date.now(), text: `Error: ${error.message}`, sender: 'system', timestamp: new Date() }]);
    } finally {
        setIsLoading(false);
        document.querySelector('.chat-input input')?.focus();
    }
  };

  const formatTimestamp = (date) => {
    return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
  };

  return (
    <div className="chat-interface-page"> {/* Renamed for clarity and page-level styling */}
      <header className="page-header chat-page-header">
        <h1>DataWise Agent Chat</h1>
      </header>
      <div className="chat-interface"> {/* Kept original for existing styles, will adjust CSS */}
        <div className="chat-messages">
          {messages.map((msg) => (
            <div key={msg.id} className={`message ${msg.sender}`}>
            <div className="message-bubble">
              <ReactMarkdown className="message-text" remarkPlugins={[remarkGfm]}>{msg.text}</ReactMarkdown>
              <span className="message-timestamp">{formatTimestamp(msg.timestamp)}</span>
            </div>
          </div>
        ))}
        {isLoading && messages[messages.length - 1]?.sender === 'user' && <TypingIndicator />} {/* Conditionally render typing indicator */}
        <div ref={messagesEndRef} />
      </div>
      <div className="suggested-questions">
        {suggestedQuestions.map((suggestion, index) => (
          <div
            key={index}
            className="suggestion-card"
            onClick={() => handleSuggestionClick(suggestion.question)}
          >
            <h4>{suggestion.heading}</h4>
            <p>{suggestion.question}</p>
          </div>
        ))}
      </div>
      <form className="chat-input-area" onSubmit={handleSendMessage}>
        <input
          type="text"
          value={inputValue}
          onChange={handleInputChange}
          placeholder="Type your message..."
          aria-label="Chat input"
          disabled={isLoading}
        />
        <button type="submit" disabled={isLoading || inputValue.trim() === ''} aria-label="Send message">
          {isLoading ? <div className="loader"></div> : <SendIcon />}
        </button>
      </form>
      </div>
    </div>
  );
};

export default ChatInterface;
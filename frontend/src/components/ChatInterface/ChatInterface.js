import React, { useState, useEffect, useRef } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import './ChatInterface.css';

// Your existing SendIcon component
const SendIcon = () => (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path d="M2.01 21L23 12L2.01 3L2 10L17 12L2 14L2.01 21Z" fill="currentColor"/>
    </svg>
);

// Your existing TypingIndicator component
const TypingIndicator = () => {
    const phrases = [ "Thinking...", "Generating a plan...", "Performing NL2SQL conversion...", "Querying BigQuery...", "Processing result..." ];
    const [currentPhraseIndex, setCurrentPhraseIndex] = useState(0);
    useEffect(() => {
        const interval = setInterval(() => {
            setCurrentPhraseIndex((prevIndex) => (prevIndex + 1) % phrases.length);
        }, 5000);
        return () => clearInterval(interval);
    }, [phrases.length]);
    return (
        <div className="message bot typing-indicator">
            <div className="message-bubble">
                <div className="typing-dots">
                    <span className="dot"></span><span className="dot"></span><span className="dot"></span>
                </div>
                <div className="typing-text">{phrases[currentPhraseIndex]}</div>
            </div>
        </div>
    );
};

// --- CHANGE 1: The component now accepts 'sessionId' as a prop from App.js ---
const ChatInterface = ({ username, sessionId }) => {
    const [messages, setMessages] = useState([
        { id: 1, text: `Hello ${username}! I am your Mahindra Data Agent. How can I assist you today?`, sender: 'bot', timestamp: new Date() }
    ]);
    const [inputValue, setInputValue] = useState('');
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState(null);
    const messagesEndRef = useRef(null);

    // --- CHANGE 2: Removed internal userId and sessionId state. They now come from props. ---

    const scrollToBottom = () => {
        messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
    };

    useEffect(() => {
        scrollToBottom();
    }, [messages]);

    const handleSendMessage = async (e) => {
        e.preventDefault();
        const messageToSend = inputValue.trim();
        if (messageToSend === '' || isLoading) return;

        const userMessage = { id: Date.now(), text: messageToSend, sender: 'user', timestamp: new Date() };
        setMessages((prev) => [...prev, userMessage]);
        setInputValue('');
        setIsLoading(true);
        setError(null);

        try {
            // --- CHANGE 3: The request body now uses the username and sessionId from props. ---
            const requestBody = {
                user_id: username,
                session_id: sessionId, 
                message: { message: messageToSend, role: 'user' }
            };
            
            console.log('Sending request to /api/chat:', requestBody);
            const response = await fetch('/api/chat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(requestBody)
            });

            if (!response.ok) {
                const errorData = await response.json().catch(() => ({ detail: "Unknown server error" }));
                throw new Error(`HTTP error! status: ${response.status} - ${errorData.error || "Failed to process message"}`);
            }

            const data = await response.json();
            console.log('Response from /api/chat:', data);
            
            // --- CHANGE 4: Removed logic to set sessionId from response. It's now fixed per session. ---

            if (data.messages && data.messages.length > 0) {
                // Map backend response {role, content} to frontend state {sender, text}
                const botReplies = data.messages.map((msg, index) => ({
                    id: Date.now() + index + 1,
                    text: msg.content,
                    sender: 'bot', // Simplified from msg.role for consistency
                    timestamp: new Date()
                }));
                setMessages((prev) => [...prev, ...botReplies]);
            } else if (data.error) {
                throw new Error(data.error);
            }
        } catch (err) {
            console.error('Error in handleSendMessage:', err);
            setError(`Failed to get response: ${err.message}`);
            setMessages((prev) => [...prev, { id: Date.now(), text: `Error: ${err.message}`, sender: 'system', timestamp: new Date() }]);
        } finally {
            setIsLoading(false);
        }
    };
    
    // Your existing timestamp formatting function
    const formatTimestamp = (date) => {
        return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    };

    // Your existing JSX layout is preserved
    return (
        <div className="chat-interface-page">
            <header className="page-header chat-page-header">
                <h1>Talk to Mahindra Data</h1>
            </header>
            <div className="chat-interface">
                <div className="chat-messages">
                    {messages.map((msg) => (
                        <div key={msg.id} className={`message ${msg.sender}`}>
                            <div className="message-bubble">
                                <div className="message-text">
                                    <ReactMarkdown remarkPlugins={[remarkGfm]}>{msg.text}</ReactMarkdown>
                                </div>
                                <span className="message-timestamp">{formatTimestamp(msg.timestamp)}</span>
                            </div>
                        </div>
                    ))}
                    {isLoading && <TypingIndicator />}
                    <div ref={messagesEndRef} />
                </div>
                
                {/* Your existing commented-out suggested questions UI is preserved */}
                
                <form className="chat-input-area" onSubmit={handleSendMessage}>
                    <input type="text" value={inputValue} onChange={(e) => setInputValue(e.target.value)} placeholder="Type your message..." disabled={isLoading} />
                    <button type="submit" disabled={isLoading || inputValue.trim() === ''}>
                        {isLoading ? <div className="loader"></div> : <SendIcon />}
                    </button>
                </form>
            </div>
        </div>
    );
};

export default ChatInterface;
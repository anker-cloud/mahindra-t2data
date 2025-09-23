import React, { useState, useEffect, useRef } from 'react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';
import './ChatInterface.css';

const SendIcon = () => (
    <svg width="24" height="24" viewBox="0 0 24 24" fill="none" xmlns="http://www.w3.org/2000/svg">
      <path d="M2.01 21L23 12L2.01 3L2 10L17 12L2 14L2.01 21Z" fill="currentColor"/>
    </svg>
);

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

// ✅ Suggested Questions Array
const suggestedQuestions = [
  { heading: 'Understanding BQ Dataset', question: 'Provide the list of Top 20 consumed part under Running Repair from 01-01-2025 to 01-06-2025 (Use Part Number as the Unique Key and the Output should be Part Number, Part Descp & Count in decending order.' },
  { heading: 'Higher Distribution Cost', question: 'List the Top 20 Dealers with highest part consumption under running repair from 2025-01-01 To 2025-06-01. (Use Parent Group Code & Location Code as unique Key, Eg. RDM01-RDM2 and do the calculation as in the description and display with the unqiue key with Dealer Name, Value in decending order.'},
  { heading: 'Problematic Product categories', question: 'List the Top Zone (Dealer Zone) & Area Office (Dealer Area Office) with highest part consumption. for full data' },
  { heading: 'Multi-Channel Attribution', question: 'List the Top 20 (MODEL_GROP) & (FAMLY_DESC) with highest part consumption between Jan 2025 to June 2025' },
  { heading: 'Most Expensive Products', question: 'Query for Top 10 Part which has been replaced in BOLERO & Bolero BS6 for entire data' },
  { heading: 'Potential Bot Attacks', question: 'Give the list of most frequently replaced parts for BOLERO & Bolero BS6 by Quantity as well as Value in West Zone, Bhopal Area Office, for the dealer SOMYA VEHICLE SOLUTIONS PVT LTD at the RASULIYA_3S location for entire data' },
];

const ChatInterface = ({ username, sessionId }) => {
    const [messages, setMessages] = useState([
        { id: 1, text: `Hello ${username}! I am your DIA. How can I assist you today?`, sender: 'bot', timestamp: new Date() }
    ]);
    const [inputValue, setInputValue] = useState('');
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState(null);
    const messagesEndRef = useRef(null);

    const scrollToBottom = () => {
        messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
    };

    useEffect(() => {
        scrollToBottom();
    }, [messages]);

    const handleSendMessage = async (e, messageString = inputValue) => {
        e.preventDefault();
        const messageToSend = messageString.trim();
        if (messageToSend === '' || isLoading) return;

        const userMessage = { id: Date.now(), text: messageToSend, sender: 'user', timestamp: new Date() };
        setMessages((prev) => [...prev, userMessage]);
        setInputValue('');
        setIsLoading(true);
        setError(null);

        try {
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

            if (data.messages && data.messages.length > 0) {
                const botReplies = data.messages.map((msg, index) => ({
                    id: Date.now() + index + 1,
                    text: msg.content,
                    sender: 'bot',
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

    const formatTimestamp = (date) => {
        return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    };

    return (
        <div className="chat-interface-page">
            <header className="page-header chat-page-header">
                <h1>Natural Language Conversation Insights</h1>
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

                {/* ✅ Suggested Questions Section */}
                <div className="suggested-questions">
                    {suggestedQuestions.map((suggestion, index) => (
                        <div
                            key={index}
                            className="suggestion-card"
                            onClick={() => handleSendMessage({ preventDefault: () => {} }, suggestion.question)}
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
                        onChange={(e) => setInputValue(e.target.value)} 
                        placeholder="Type your message..." 
                        disabled={isLoading} 
                    />
                    <button type="submit" disabled={isLoading || inputValue.trim() === ''}>
                        {isLoading ? <div className="loader"></div> : <SendIcon />}
                    </button>
                </form>
            </div>
        </div>
    );
};

export default ChatInterface;
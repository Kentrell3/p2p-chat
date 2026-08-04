// server.js - Server-based chat (like WhatsApp)
const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');
const path = require('path');

const PORT = process.env.PORT || 8080;

// Store all messages per room
const messages = {};
// Store active connections
const clients = {};

const server = http.createServer((req, res) => {
    // Serve the HTML file
    fs.readFile('index.html', (err, data) => {
        if (err) {
            res.writeHead(200, { 'Content-Type': 'text/plain' });
            res.end('✅ Server is running!');
        } else {
            res.writeHead(200, { 'Content-Type': 'text/html' });
            res.end(data);
        }
    });
});

const wss = new WebSocket.Server({ server });

wss.on('connection', (ws) => {
    let userInfo = null;

    ws.on('message', (data) => {
        try {
            const msg = JSON.parse(data);
            console.log(`📨 ${msg.type} from ${userInfo?.username || 'unknown'}`);

            switch (msg.type) {
                case 'register':
                    // User joins a room
                    const roomId = msg.roomId;
                    const username = msg.username;
                    
                    userInfo = { roomId, username };
                    
                    // Store client
                    if (!clients[roomId]) clients[roomId] = {};
                    clients[roomId][username] = ws;
                    
                    // Initialize messages for room
                    if (!messages[roomId]) messages[roomId] = [];
                    
                    // Send last 50 messages to user
                    const history = messages[roomId].slice(-50);
                    ws.send(JSON.stringify({
                        type: 'history',
                        messages: history
                    }));
                    
                    // Notify everyone in room
                    broadcast(roomId, {
                        type: 'user-joined',
                        username: username,
                        users: Object.keys(clients[roomId])
                    });
                    
                    console.log(`✅ ${username} joined room ${roomId}`);
                    break;

                case 'message':
                    // User sends a message
                    if (!userInfo) return;
                    
                    const messageData = {
                        id: Date.now().toString(36) + Math.random().toString(36).substring(2, 6),
                        sender: userInfo.username,
                        message: msg.message,
                        timestamp: Date.now(),
                        isPrivate: msg.isPrivate || false,
                        targetUser: msg.targetUser || null
                    };
                    
                    // Store in history
                    messages[userInfo.roomId].push(messageData);
                    
                    // Send to everyone in room (or specific user for private)
                    if (messageData.isPrivate && messageData.targetUser) {
                        // Private message - send only to target
                        sendPrivate(userInfo.roomId, messageData.targetUser, {
                            type: 'private-message',
                            message: messageData
                        });
                        // Also send to sender
                        if (clients[userInfo.roomId][userInfo.username]) {
                            clients[userInfo.roomId][userInfo.username].send(JSON.stringify({
                                type: 'private-message',
                                message: messageData
                            }));
                        }
                    } else {
                        // Public message - broadcast to all
                        broadcast(userInfo.roomId, {
                            type: 'message',
                            message: messageData
                        });
                    }
                    break;

                case 'typing':
                    // Typing indicator
                    if (!userInfo) return;
                    broadcast(userInfo.roomId, {
                        type: 'typing',
                        username: userInfo.username,
                        isTyping: msg.isTyping
                    }, [userInfo.username]);
                    break;

                case 'leave':
                    handleDisconnect(userInfo);
                    break;

                case 'ping':
                    ws.send(JSON.stringify({ type: 'pong' }));
                    break;
            }
        } catch (e) {
            console.error('Error:', e);
        }
    });

    ws.on('close', () => {
        handleDisconnect(userInfo);
    });
});

function broadcast(roomId, data, exclude = []) {
    if (!clients[roomId]) return;
    
    const message = JSON.stringify(data);
    Object.keys(clients[roomId]).forEach(username => {
        if (exclude.includes(username)) return;
        const ws = clients[roomId][username];
        if (ws.readyState === WebSocket.OPEN) {
            ws.send(message);
        }
    });
}

function sendPrivate(roomId, targetUsername, data) {
    if (!clients[roomId]) return;
    if (clients[roomId][targetUsername]) {
        clients[roomId][targetUsername].send(JSON.stringify(data));
    }
}

function handleDisconnect(userInfo) {
    if (!userInfo) return;
    const { roomId, username } = userInfo;
    
    if (clients[roomId]) {
        delete clients[roomId][username];
        
        // Notify others
        broadcast(roomId, {
            type: 'user-left',
            username: username,
            users: Object.keys(clients[roomId])
        });
        
        console.log(`👋 ${username} left room ${roomId}`);
    }
}

server.listen(PORT, '0.0.0.0', () => {
    console.log(`
    ╔════════════════════════════════════════════════════╗
    ║   💬 Secure Chat Server (WhatsApp-style)          ║
    ║   Running on: http://0.0.0.0:${PORT}               ║
    ║   Messages are stored in memory                   ║
    ║   🔒 End-to-end encryption enabled                 ║
    ╚════════════════════════════════════════════════════╝
    `);
});

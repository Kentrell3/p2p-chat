// server.js - With file support
const WebSocket = require('ws');
const http = require('http');
const fs = require('fs');

const PORT = process.env.PORT || 8080;

const messages = {};
const clients = {};

const server = http.createServer((req, res) => {
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
                    const roomId = msg.roomId;
                    const username = msg.username;
                    
                    userInfo = { roomId, username };
                    
                    if (!clients[roomId]) clients[roomId] = {};
                    clients[roomId][username] = ws;
                    
                    if (!messages[roomId]) messages[roomId] = [];
                    
                    const history = messages[roomId].slice(-50);
                    ws.send(JSON.stringify({
                        type: 'history',
                        messages: history
                    }));
                    
                    broadcast(roomId, {
                        type: 'user-joined',
                        users: Object.keys(clients[roomId])
                    });
                    
                    console.log(`✅ ${username} joined room ${roomId}`);
                    break;

                case 'message':
                    if (!userInfo) return;
                    
                    const messageData = {
                        id: msg.id || Date.now().toString(36) + Math.random().toString(36).substring(2, 6),
                        sender: userInfo.username,
                        message: msg.message,
                        timestamp: Date.now(),
                        isPrivate: msg.isPrivate || false,
                        targetUser: msg.targetUser || null
                    };
                    
                    messages[userInfo.roomId].push(messageData);
                    
                    if (messageData.isPrivate && messageData.targetUser) {
                        sendPrivate(userInfo.roomId, messageData.targetUser, {
                            type: 'private-message',
                            message: messageData
                        });
                        if (clients[userInfo.roomId][userInfo.username]) {
                            clients[userInfo.roomId][userInfo.username].send(JSON.stringify({
                                type: 'private-message',
                                message: messageData
                            }));
                        }
                    } else {
                        broadcast(userInfo.roomId, {
                            type: 'message',
                            message: messageData
                        });
                    }
                    break;

                case 'file':
                    if (!userInfo) return;
                    
                    const fileData = {
                        id: msg.id || Date.now().toString(36) + Math.random().toString(36).substring(2, 6),
                        sender: userInfo.username,
                        fileName: msg.fileName,
                        fileSize: msg.fileSize,
                        fileType: msg.fileType,
                        fileData: msg.fileData,
                        timestamp: Date.now(),
                        isPrivate: msg.isPrivate || false,
                        targetUser: msg.targetUser || null,
                        isFile: true
                    };
                    
                    messages[userInfo.roomId].push(fileData);
                    
                    if (fileData.isPrivate && fileData.targetUser) {
                        sendPrivate(userInfo.roomId, fileData.targetUser, {
                            type: 'file',
                            message: fileData
                        });
                        if (clients[userInfo.roomId][userInfo.username]) {
                            clients[userInfo.roomId][userInfo.username].send(JSON.stringify({
                                type: 'file',
                                message: fileData
                            }));
                        }
                    } else {
                        broadcast(userInfo.roomId, {
                            type: 'file',
                            message: fileData
                        });
                    }
                    break;

                case 'typing':
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
        broadcast(roomId, {
            type: 'user-left',
            users: Object.keys(clients[roomId])
        });
        console.log(`👋 ${username} left room ${roomId}`);
    }
}

server.listen(PORT, '0.0.0.0', () => {
    console.log(`
    ╔════════════════════════════════════════════════════╗
    ║   💬 Secure Chat Server                           ║
    ║   Running on: http://0.0.0.0:${PORT}               ║
    ║   🔒 End-to-end encryption                        ║
    ║   📎 File sharing supported                       ║
    ╚════════════════════════════════════════════════════╝
    `);
});

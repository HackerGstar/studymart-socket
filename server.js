// StudyMart Socket.IO Server
// Run with: node server.js
// Or with PM2: pm2 start server.js --name studymart-socket

const http = require('http');
const socketIo = require('socket.io');
const mysql = require('mysql2/promise');
const crypto = require('crypto');

// Configuration
const PORT = process.env.PORT || 3000;
const SECRET_KEY = 'studymart-socket-secret-key-2024';

// Database connection pool
let dbPool = null;

// Store connected users: userId -> { userId, username, fullName, avatar, socketIds }
const connectedUsers = new Map();

// Store typing status: room -> { userId, timeout }
const typingUsers = new Map();

// Store active quiz rooms
const activeQuizRooms = new Map();
const roomTimers = new Map();

// Create HTTP server
const server = http.createServer((req, res) => {
    // Health check endpoint
    if (req.url === '/health') {
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ status: 'ok', timestamp: new Date().toISOString(), connections: connectedUsers.size }));
        return;
    }
    
    // Simple status page
    if (req.url === '/status') {
        res.writeHead(200, { 'Content-Type': 'text/html' });
        res.end(`
            <!DOCTYPE html>
            <html>
            <head><title>StudyMart Socket Server</title></head>
            <body>
                <h1>StudyMart Socket.IO Server</h1>
                <p>Status: Running</p>
                <p>Connected Users: ${connectedUsers.size}</p>
                <p>Active Quiz Rooms: ${activeQuizRooms.size}</p>
                <p>Port: ${PORT}</p>
                <p>Uptime: ${Math.floor(process.uptime())} seconds</p>
            </body>
            </html>
        `);
        return;
    }
    
    res.writeHead(404);
    res.end();
});

// Initialize Socket.IO with CORS settings
const io = socketIo(server, {
    cors: {
        origin: true,
        methods: ['GET', 'POST'],
        credentials: true,
        allowedHeaders: ['Content-Type', 'Authorization']
    },
    transports: ['websocket', 'polling'],
    pingTimeout: 60000,
    pingInterval: 25000
});

// Helper function to generate room code
function generateRoomCode() {
    return Math.random().toString(36).substring(2, 8).toUpperCase();
}

// Initialize database connection
async function initDatabase() {
    try {
        dbPool = await mysql.createPool({
            host: 'localhost',
            user: 'root',
            password: '',
            database: 'studymart',
            waitForConnections: true,
            connectionLimit: 10,
            queueLimit: 0
        });
        console.log('✅ MySQL connection pool created');
        
        const [rows] = await dbPool.query('SELECT 1 as test');
        console.log('✅ Database connected');
        
        return true;
    } catch (error) {
        console.error('❌ Database connection failed:', error.message);
        return false;
    }
}

// Generate room name for 1-on-1 chat
function getPrivateRoom(userId1, userId2) {
    const sorted = [userId1, userId2].sort();
    return `private_${sorted[0]}_${sorted[1]}`;
}

// Generate room name for group chat
function getGroupRoom(groupId) {
    return `group_${groupId}`;
}

// Update user online status in database
async function updateUserOnlineStatus(userId, isOnline) {
    if (!dbPool) return;
    try {
        await dbPool.query(
            'UPDATE users SET is_online = ?, last_seen = NOW(), last_activity = NOW() WHERE id = ?',
            [isOnline ? 1 : 0, userId]
        );
    } catch (error) {
        console.error('Error updating online status:', error.message);
    }
}

// Get user's privacy settings
async function getUserPrivacySettings(userId) {
    if (!dbPool) return { show_online: 1, show_last_seen: 1, message_permission: 'everyone' };
    try {
        const [rows] = await dbPool.query(
            'SELECT show_online, show_last_seen, message_permission FROM user_preferences WHERE user_id = ?',
            [userId]
        );
        if (rows.length > 0) return rows[0];
        return { show_online: 1, show_last_seen: 1, message_permission: 'everyone' };
    } catch (error) {
        return { show_online: 1, show_last_seen: 1, message_permission: 'everyone' };
    }
}

// Notify friends about user's online status
async function notifyOnlineStatus(userId, isOnline, userData) {
    if (!dbPool) return;
    try {
        const [conversations] = await dbPool.query(`
            SELECT DISTINCT 
                CASE 
                    WHEN from_user_id = ? THEN to_user_id
                    WHEN to_user_id = ? THEN from_user_id
                END as friend_id
            FROM messages
            WHERE from_user_id = ? OR to_user_id = ?
        `, [userId, userId, userId, userId]);
        
        const privacy = await getUserPrivacySettings(userId);
        
        for (const conv of conversations) {
            const friendId = conv.friend_id;
            if (friendId && connectedUsers.has(friendId)) {
                const friendSockets = connectedUsers.get(friendId);
                const showOnline = privacy.show_online == 1;
                for (const socketId of friendSockets.socketIds) {
                    io.to(socketId).emit('user_status_change', {
                        user_id: userId,
                        is_online: isOnline && showOnline,
                        last_seen: isOnline ? null : new Date().toISOString(),
                        username: userData?.username || '',
                        full_name: userData?.full_name || ''
                    });
                }
            }
        }
    } catch (error) {
        console.error('Error notifying online status:', error.message);
    }
}

// Mark messages as read - only when user actually opens the chat
async function markMessagesAsRead(userId, fromUserId) {
    if (!dbPool) return;
    try {
        const [result] = await dbPool.query(
            'UPDATE messages SET is_read = 1, read_at = NOW() WHERE from_user_id = ? AND to_user_id = ? AND is_read = 0',
            [fromUserId, userId]
        );
        
        if (result.affectedRows > 0 && connectedUsers.has(fromUserId)) {
            const senderSockets = connectedUsers.get(fromUserId);
            for (const socketId of senderSockets.socketIds) {
                io.to(socketId).emit('messages_read', {
                    from_user_id: fromUserId,
                    to_user_id: userId,
                    read_at: new Date().toISOString()
                });
            }
        }
        return result.affectedRows;
    } catch (error) {
        console.error('Error marking messages as read:', error.message);
        return 0;
    }
}

// End quiz exam function
async function endQuizExam(roomId) {
    const room = activeQuizRooms.get(roomId);
    if (!room || room.status === 'ended') return;
    
    room.status = 'ended';
    room.ended_at = new Date();
    
    // Stop timer
    const timer = roomTimers.get(roomId);
    if (timer) clearInterval(timer);
    roomTimers.delete(roomId);
    
    // Calculate leaderboard
    const sortedParticipants = [...room.participants].sort((a, b) => b.score - a.score);
    
    // Prepare correct answers
    const correctAnswers = room.questions.map(q => ({
        question: q.question,
        correct_answer: q.correct,
        explanation: q.explanation || ''
    }));
    
    const results = {
        leaderboard: sortedParticipants.map(p => ({
            user_id: p.user_id,
            full_name: p.full_name,
            score: p.score
        })),
        correct_answers: correctAnswers,
        total_questions: room.questions.length
    };
    
    // Add current user's score for each participant
    for (const participant of room.participants) {
        const userSockets = connectedUsers.get(participant.user_id);
        if (userSockets) {
            const participantResults = {
                ...results,
                your_score: participant.score
            };
            for (const socketId of userSockets.socketIds) {
                io.to(socketId).emit('exam_ended', participantResults);
            }
        }
    }
    
    // Clean up after delay
    setTimeout(() => {
        activeQuizRooms.delete(roomId);
    }, 60000);
}

// Socket.IO event handlers
io.on('connection', async (socket) => {
    console.log(`🔌 New connection: ${socket.id}`);
    
    let currentUserId = null;
    let currentUserData = null;
    
    // Handle user authentication
    socket.on('authenticate', async (data) => {
        const { user_id, username, full_name, avatar } = data;
        
        if (!user_id) {
            socket.emit('auth_error', { message: 'User ID required' });
            return;
        }
        
        currentUserId = user_id;
        currentUserData = { id: user_id, username, full_name, avatar };
        
        if (!connectedUsers.has(user_id)) {
            connectedUsers.set(user_id, {
                userId: user_id,
                username: username,
                full_name: full_name,
                avatar: avatar,
                socketIds: new Set()
            });
        }
        
        const userConnections = connectedUsers.get(user_id);
        userConnections.socketIds.add(socket.id);
        userConnections.username = username;
        userConnections.full_name = full_name;
        userConnections.avatar = avatar;
        
        socket.join(`user_${user_id}`);
        
        console.log(`✅ User ${username} (ID: ${user_id}) authenticated. Total connections: ${connectedUsers.size}`);
        
        await updateUserOnlineStatus(user_id, true);
        await notifyOnlineStatus(user_id, true, { username, full_name });
        
        socket.emit('authenticated', {
            success: true,
            user_id: user_id,
            message: 'Authenticated successfully'
        });
    });
    
    // Send notification to a specific user
    socket.on('send_notification', (data) => {
        const { user_id, notification } = data;
        if (!user_id || !notification) return;
        if (connectedUsers.has(user_id)) {
            const userSockets = connectedUsers.get(user_id);
            for (const socketId of userSockets.socketIds) {
                io.to(socketId).emit('new_notification', notification);
            }
        }
    });
    
    // Mark notification as read
    socket.on('mark_notification_read', (data) => {
        const { notification_id, user_id } = data;
        if (!notification_id || !user_id) return;
        if (connectedUsers.has(user_id)) {
            const userSockets = connectedUsers.get(user_id);
            for (const socketId of userSockets.socketIds) {
                io.to(socketId).emit('notification_read', { notification_id });
            }
        }
    });
    
    // Get unread notification count
    socket.on('get_unread_notifications_count', async (data) => {
        const { user_id } = data;
        if (!user_id || !dbPool) return;
        try {
            const [rows] = await dbPool.query(
                'SELECT COUNT(*) as count FROM notifications WHERE user_id = ? AND is_read = 0',
                [user_id]
            );
            socket.emit('unread_notifications_count', { count: rows[0].count, user_id: user_id });
        } catch (error) {
            console.error('Error getting unread count:', error.message);
        }
    });

    // Post score update
    socket.on('post_score_update', (data) => {
        const { post_id, score, like_count, comment_count } = data;
        io.to(`post_${post_id}`).emit('post_score_update', {
            post_id, score, like_count, comment_count
        });
    });
    
    // Join post room
    socket.on('join_post_room', (data) => {
        const { post_id } = data;
        if (post_id) {
            socket.join(`post_${post_id}`);
        }
    });

    // Get online users
    socket.on('get_online_users', () => {
        const onlineUsers = [];
        for (const [userId, userData] of connectedUsers) {
            onlineUsers.push({
                user_id: userId,
                username: userData.username,
                full_name: userData.full_name,
                avatar: userData.avatar
            });
        }
        socket.emit('online_users', {
            success: true,
            users: onlineUsers,
            count: onlineUsers.length,
            timestamp: new Date().toISOString()
        });
    });
    
    // Join private chat room
    socket.on('join_private_chat', async (data) => {
        const { other_user_id } = data;
        if (!currentUserId) {
            socket.emit('error', { message: 'Not authenticated' });
            return;
        }
        const roomName = getPrivateRoom(currentUserId, other_user_id);
        socket.join(roomName);
        const otherUserOnline = connectedUsers.has(other_user_id);
        socket.emit('room_joined', {
            room: roomName,
            other_user_online: otherUserOnline,
            other_user_id: other_user_id
        });
    });
    
    // Join group chat room
    socket.on('join_group_chat', async (data) => {
        const { group_id } = data;
        if (!currentUserId) {
            socket.emit('error', { message: 'Not authenticated' });
            return;
        }
        const roomName = getGroupRoom(group_id);
        socket.join(roomName);
        socket.emit('room_joined', { room: roomName, group_id: group_id });
    });
    
    // Leave room
    socket.on('leave_room', (data) => {
        const { room } = data;
        if (room) socket.leave(room);
    });
    
    // ============================================================
    // SEND PRIVATE MESSAGE - WITH BLOCK CHECK AND READ RECEIPT FIX
    // ============================================================
    socket.on('send_private_message', async (data) => {
        const { to_user_id, message, reply_to_id, temp_id } = data;
        
        if (!currentUserId) {
            socket.emit('error', { message: 'Not authenticated' });
            return;
        }
        
        if (!message || !message.trim()) {
            socket.emit('error', { message: 'Message cannot be empty' });
            return;
        }
        
        // CHECK IF BLOCKED
        try {
            const [blockedRows] = await dbPool.query(
                `SELECT id FROM blocked_users 
                 WHERE (user_id = ? AND blocked_user_id = ?) 
                    OR (user_id = ? AND blocked_user_id = ?)
                 LIMIT 1`,
                [currentUserId, to_user_id, to_user_id, currentUserId]
            );
            
            if (blockedRows.length > 0) {
                socket.emit('message_error', {
                    success: false,
                    error: 'Cannot send message. User may be blocked.',
                    temp_id: temp_id
                });
                console.log(`🚫 Blocked message attempt: ${currentUserId} -> ${to_user_id}`);
                return;
            }
        } catch (blockErr) {
            console.error('Error checking block status:', blockErr.message);
        }
        // END BLOCK CHECK
        
        const recipientOnline = connectedUsers.has(to_user_id);
        
        try {
            // FIXED: Always insert as unread (0) - only mark read when user opens chat
            const [result] = await dbPool.query(
                `INSERT INTO messages (from_user_id, to_user_id, message, reply_to_id, is_read, created_at) 
                 VALUES (?, ?, ?, ?, 0, NOW())`,
                [currentUserId, to_user_id, message, reply_to_id || null]
            );
            
            const messageId = result.insertId;
            
            if (!messageId) {
                throw new Error('Failed to insert message');
            }
            
            const [userRows] = await dbPool.query(
                `SELECT id, full_name, avatar, username, is_verified FROM users WHERE id = ?`,
                [currentUserId]
            );
            
            if (!userRows || userRows.length === 0) {
                throw new Error('User not found');
            }
            
            const userData = userRows[0];
            
            const [msgRows] = await dbPool.query(
                `SELECT id, from_user_id, to_user_id, message, is_read, created_at, 
                        image_url, voice_url, voice_duration, reply_to_id
                 FROM messages WHERE id = ?`,
                [messageId]
            );
            
            if (!msgRows || msgRows.length === 0) {
                throw new Error('Failed to retrieve inserted message');
            }
            
            const messageData = msgRows[0];
            
            const newMessage = {
                ...messageData,
                full_name: userData.full_name,
                avatar: userData.avatar,
                username: userData.username,
                is_verified: userData.is_verified,
                is_mine: true,
                reactions: []
            };
            
            // Handle image_url safely
            if (newMessage.image_url && typeof newMessage.image_url === 'string' && 
                !newMessage.image_url.startsWith('http') && !newMessage.image_url.startsWith('/studymart') && 
                newMessage.image_url !== 'NULL' && newMessage.image_url !== '') {
                newMessage.image_url = '/studymart/' + newMessage.image_url;
            } else if (!newMessage.image_url || newMessage.image_url === 'NULL') {
                newMessage.image_url = null;
            }
            
            // Handle voice_url safely
            if (newMessage.voice_url && typeof newMessage.voice_url === 'string' &&
                !newMessage.voice_url.startsWith('http') && !newMessage.voice_url.startsWith('/studymart') &&
                newMessage.voice_url !== 'NULL' && newMessage.voice_url !== '') {
                newMessage.voice_url = '/studymart/' + newMessage.voice_url;
            } else if (!newMessage.voice_url || newMessage.voice_url === 'NULL') {
                newMessage.voice_url = null;
            }
            
            // Get reply message if exists
            let replyToMessage = null;
            if (reply_to_id && reply_to_id > 0) {
                try {
                    const [replyRows] = await dbPool.query(`
                        SELECT m.id, m.message, m.image_url, m.voice_url, m.voice_duration, m.from_user_id, m.created_at,
                               u.full_name, u.avatar, u.username
                        FROM messages m
                        LEFT JOIN users u ON m.from_user_id = u.id
                        WHERE m.id = ?
                    `, [reply_to_id]);
                    
                    if (replyRows && replyRows.length > 0) {
                        const quotedMsg = replyRows[0];
                        replyToMessage = {
                            id: quotedMsg.id,
                            message: quotedMsg.message || '',
                            image_url: quotedMsg.image_url || null,
                            voice_url: quotedMsg.voice_url || null,
                            voice_duration: quotedMsg.voice_duration || 0,
                            is_mine: (quotedMsg.from_user_id == currentUserId),
                            full_name: quotedMsg.full_name || 'User',
                            created_at: quotedMsg.created_at
                        };
                    }
                } catch (replyErr) {
                    console.error('Error fetching reply message:', replyErr.message);
                }
            }
            
            const messageToSend = {
                ...newMessage,
                reply_to_message: replyToMessage,
                temp_id: temp_id
            };
            
            // Send confirmation to sender
            socket.emit('message_sent', {
                success: true,
                message: messageToSend,
                temp_id: temp_id
            });
            
            // Send to recipient if online
            if (recipientOnline) {
                const recipientSockets = connectedUsers.get(to_user_id);
                if (recipientSockets && recipientSockets.socketIds) {
                    for (const socketId of recipientSockets.socketIds) {
                        io.to(socketId).emit('new_private_message', {
                            ...messageToSend,
                            is_mine: false
                        });
                    }
                }
            }
            
            console.log(`💬 Private message sent: ${currentUserId} -> ${to_user_id}, Message ID: ${messageId}`);
            
        } catch (error) {
            console.error('Error sending private message:', error.message);
            socket.emit('message_error', {
                success: false,
                error: 'Failed to send message: ' + error.message,
                temp_id: temp_id
            });
        }
    });
    
    // Handle sending a group message
    socket.on('send_group_message', async (data) => {
        const { group_id, message, reply_to_id, temp_id } = data;
        
        if (!currentUserId) {
            socket.emit('error', { message: 'Not authenticated' });
            return;
        }
        
        if (!message || !message.trim()) {
            socket.emit('error', { message: 'Message cannot be empty' });
            return;
        }
        
        try {
            const [result] = await dbPool.query(
                `INSERT INTO group_messages (group_id, user_id, message, reply_to_id, created_at) 
                 VALUES (?, ?, ?, ?, NOW())`,
                [group_id, currentUserId, message, reply_to_id || null]
            );
            
            const messageId = result.insertId;
            
            const [rows] = await dbPool.query(`
                SELECT gm.*, u.full_name, u.avatar, u.username, u.is_verified
                FROM group_messages gm
                JOIN users u ON gm.user_id = u.id
                WHERE gm.id = ?
            `, [messageId]);
            
            const newMessage = rows[0];
            
            if (newMessage.image_url && !newMessage.image_url.startsWith('http') && !newMessage.image_url.startsWith('/studymart')) {
                newMessage.image_url = '/studymart/' + newMessage.image_url;
            }
            if (newMessage.voice_url && !newMessage.voice_url.startsWith('http') && !newMessage.voice_url.startsWith('/studymart')) {
                newMessage.voice_url = '/studymart/' + newMessage.voice_url;
            }
            
            newMessage.is_mine = true;
            
            let replyToMessage = null;
            if (reply_to_id) {
                const [replyRows] = await dbPool.query(`
                    SELECT gm.*, u.full_name, u.avatar, u.username
                    FROM group_messages gm
                    LEFT JOIN users u ON gm.user_id = u.id
                    WHERE gm.id = ?
                `, [reply_to_id]);
                
                if (replyRows.length > 0) {
                    const quotedMsg = replyRows[0];
                    if (quotedMsg.image_url && !quotedMsg.image_url.startsWith('http') && !quotedMsg.image_url.startsWith('/studymart')) {
                        quotedMsg.image_url = '/studymart/' + quotedMsg.image_url;
                    }
                    if (quotedMsg.voice_url && !quotedMsg.voice_url.startsWith('http') && !quotedMsg.voice_url.startsWith('/studymart')) {
                        quotedMsg.voice_url = '/studymart/' + quotedMsg.voice_url;
                    }
                    replyToMessage = {
                        id: quotedMsg.id,
                        message: quotedMsg.message,
                        image_url: quotedMsg.image_url,
                        voice_url: quotedMsg.voice_url,
                        voice_duration: quotedMsg.voice_duration,
                        is_mine: (quotedMsg.user_id == currentUserId),
                        full_name: quotedMsg.full_name || 'User',
                        created_at: quotedMsg.created_at
                    };
                }
            }
            
            const messageToSend = {
                ...newMessage,
                reply_to_message: replyToMessage,
                temp_id: temp_id
            };
            
            socket.emit('group_message_sent', {
                success: true,
                message: messageToSend,
                temp_id: temp_id
            });
            
            const groupRoom = getGroupRoom(group_id);
            io.to(groupRoom).emit('new_group_message', {
                ...messageToSend,
                is_mine: false
            });
            
            console.log(`👥 Group message sent: ${currentUserId} -> group ${group_id}`);
            
        } catch (error) {
            console.error('Error sending group message:', error.message);
            socket.emit('message_error', {
                success: false,
                error: 'Failed to send group message',
                temp_id: temp_id
            });
        }
    });
    
    // Handle typing indicator
    socket.on('typing', async (data) => {
        const { to_user_id, is_typing, group_id } = data;
        if (!currentUserId) return;
        
        if (to_user_id) {
            const recipientOnline = connectedUsers.has(to_user_id);
            if (recipientOnline) {
                const recipientSockets = connectedUsers.get(to_user_id);
                for (const socketId of recipientSockets.socketIds) {
                    io.to(socketId).emit('user_typing', {
                        from_user_id: currentUserId,
                        is_typing: is_typing,
                        username: currentUserData?.username || ''
                    });
                }
            }
        } else if (group_id) {
            const groupRoom = getGroupRoom(group_id);
            socket.to(groupRoom).emit('user_typing_group', {
                user_id: currentUserId,
                is_typing: is_typing,
                username: currentUserData?.username || '',
                full_name: currentUserData?.full_name || ''
            });
        }
    });
    
    // Handle read receipt - only when user explicitly marks as read
    socket.on('mark_read', async (data) => {
        const { from_user_id, group_id } = data;
        if (!currentUserId) return;
        
        if (from_user_id) {
            await markMessagesAsRead(currentUserId, from_user_id);
            
            const senderOnline = connectedUsers.has(from_user_id);
            if (senderOnline) {
                const senderSockets = connectedUsers.get(from_user_id);
                for (const socketId of senderSockets.socketIds) {
                    io.to(socketId).emit('messages_read', {
                        from_user_id: from_user_id,
                        to_user_id: currentUserId,
                        read_at: new Date().toISOString()
                    });
                }
            }
        } else if (group_id) {
            try {
                await dbPool.query(
                    `INSERT INTO chat_group_read_status (user_id, group_id, last_read_at, updated_at)
                     VALUES (?, ?, NOW(), NOW())
                     ON DUPLICATE KEY UPDATE last_read_at = NOW(), updated_at = NOW()`,
                    [currentUserId, group_id]
                );
            } catch (error) {
                console.error('Error marking group as read:', error.message);
            }
        }
    });
    
    // ============================================================
    // QUIZ ROOM EVENTS
    // ============================================================
    
    // Create quiz room
    socket.on('create_quiz_room', async (data) => {
        if (!currentUserId) {
            socket.emit('error', { message: 'Not authenticated' });
            return;
        }
        
        const roomCode = generateRoomCode();
        const roomId = Date.now();
        
        const roomData = {
            id: roomId,
            room_code: roomCode,
            topic: data.topic,
            difficulty: data.difficulty,
            question_type: data.question_type,
            question_count: data.question_count,
            time_limit: data.time_limit,
            questions: data.questions,
            creator_id: currentUserId,
            status: 'waiting',
            participants: [{
                user_id: currentUserId,
                full_name: currentUserData?.full_name || 'User',
                username: currentUserData?.username,
                avatar: currentUserData?.avatar,
                submitted: false,
                score: 0,
                answers: []
            }],
            created_at: new Date()
        };
        
        activeQuizRooms.set(roomId, roomData);
        socket.join(`quiz_room_${roomId}`);
        
        socket.emit('room_created', {
            room: {
                id: roomId,
                room_code: roomCode,
                topic: data.topic,
                time_limit: data.time_limit
            },
            participants: roomData.participants
        });
    });

    // Join quiz room
    socket.on('join_quiz_room', async (data) => {
        if (!currentUserId) {
            socket.emit('error', { message: 'Not authenticated' });
            return;
        }
        
        let foundRoom = null;
        for (const [id, room] of activeQuizRooms) {
            if (room.room_code === data.room_code && room.status === 'waiting') {
                foundRoom = room;
                break;
            }
        }
        
        if (!foundRoom) {
            socket.emit('error', { message: 'Room not found or already started' });
            return;
        }
        
        const alreadyInRoom = foundRoom.participants.some(p => p.user_id == currentUserId);
        if (!alreadyInRoom) {
            foundRoom.participants.push({
                user_id: currentUserId,
                full_name: currentUserData?.full_name || 'User',
                username: currentUserData?.username,
                avatar: currentUserData?.avatar,
                submitted: false,
                score: 0,
                answers: []
            });
        }
        
        socket.join(`quiz_room_${foundRoom.id}`);
        
        io.to(`quiz_room_${foundRoom.id}`).emit('participant_joined', {
            participants: foundRoom.participants.map(p => ({
                user_id: p.user_id,
                full_name: p.full_name,
                username: p.username,
                avatar: p.avatar,
                submitted: p.submitted
            }))
        });
        
        socket.emit('room_joined', {
            room: {
                id: foundRoom.id,
                room_code: foundRoom.room_code,
                topic: foundRoom.topic,
                time_limit: foundRoom.time_limit
            },
            participants: foundRoom.participants.map(p => ({
                user_id: p.user_id,
                full_name: p.full_name,
                username: p.username,
                avatar: p.avatar,
                submitted: p.submitted
            }))
        });
    });

    // Start quiz exam
    socket.on('start_quiz_exam', async (data) => {
        if (!currentUserId) return;
        
        const room = activeQuizRooms.get(data.room_id);
        if (!room || room.creator_id != currentUserId) {
            socket.emit('error', { message: 'Only room creator can start exam' });
            return;
        }
        
        room.status = 'active';
        room.started_at = new Date();
        
        io.to(`quiz_room_${room.id}`).emit('exam_started', {
            questions: room.questions,
            room: {
                id: room.id,
                room_code: room.room_code,
                topic: room.topic,
                time_limit: room.time_limit
            },
            time_limit: room.time_limit
        });
        
        // Start timer
        let timeLeft = room.time_limit * 60;
        const timer = setInterval(() => {
            if (timeLeft <= 0) {
                clearInterval(timer);
                roomTimers.delete(room.id);
                endQuizExam(room.id);
            } else {
                timeLeft--;
                io.to(`quiz_room_${room.id}`).emit('timer_update', { time_left: timeLeft });
            }
        }, 1000);
        
        roomTimers.set(room.id, timer);
    });

    // Submit quiz answer
    socket.on('submit_quiz_answer', async (data) => {
        if (!currentUserId) return;
        
        const room = activeQuizRooms.get(data.room_id);
        if (!room) return;
        
        const participant = room.participants.find(p => p.user_id == currentUserId);
        if (participant && !participant.submitted) {
            participant.answers = data.answers;
            participant.score = data.score;
            participant.submitted = true;
            participant.submitted_at = new Date();
            
            io.to(`quiz_room_${room.id}`).emit('participant_submitted', {
                user_id: currentUserId,
                full_name: currentUserData?.full_name
            });
            
            // Check if all participants have submitted
            const allSubmitted = room.participants.every(p => p.submitted === true);
            if (allSubmitted && room.status === 'active') {
                endQuizExam(room.id);
            }
        }
    });
    
    // Handle disconnection
    socket.on('disconnect', async () => {
        console.log(`🔌 Disconnected: ${socket.id}`);
        
        if (currentUserId) {
            const userConnections = connectedUsers.get(currentUserId);
            if (userConnections) {
                userConnections.socketIds.delete(socket.id);
                
                if (userConnections.socketIds.size === 0) {
                    connectedUsers.delete(currentUserId);
                    await updateUserOnlineStatus(currentUserId, false);
                    await notifyOnlineStatus(currentUserId, false, currentUserData);
                    console.log(`📡 User ${currentUserId} is now offline`);
                }
            }
        }
        console.log(`Total connections remaining: ${connectedUsers.size}`);
    });
});

// Start the server
async function startServer() {
    const dbConnected = await initDatabase();
    if (!dbConnected) {
        console.error('❌ Failed to connect to database. Server will run but features may be limited.');
    }
    
    server.listen(PORT, () => {
        console.log(`🚀 Socket.IO server running on port ${PORT}`);
        console.log(`📍 Health check: http://localhost:${PORT}/health`);
        console.log(`📍 Status page: http://localhost:${PORT}/status`);
        console.log(`📡 WebSocket endpoint: ws://localhost:${PORT}`);
    });
}

startServer();

// Graceful shutdown
process.on('SIGINT', async () => {
    console.log('🛑 Shutting down...');
    for (const [userId, userConnections] of connectedUsers) {
        await updateUserOnlineStatus(userId, false);
    }
    if (dbPool) {
        await dbPool.end();
        console.log('Database pool closed');
    }
    process.exit(0);
});
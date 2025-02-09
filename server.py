import asyncio
import json
import sqlite3
from datetime import datetime
import logging
from pathlib import Path
from aiohttp import web
from aiohttp_middlewares import cors_middleware
import os

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self, db_path='chat.db'):
        self.db_path = db_path
        self._init_db()

    def _init_db(self):
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            
            c.execute('''CREATE TABLE IF NOT EXISTS messages
                        (id INTEGER PRIMARY KEY AUTOINCREMENT,
                        username TEXT NOT NULL,
                        avatar TEXT NOT NULL,
                        content TEXT NOT NULL,
                        timestamp TEXT NOT NULL,
                        ip_address TEXT)''')
            
            c.execute('''CREATE TABLE IF NOT EXISTS users
                        (ip_address TEXT PRIMARY KEY,
                        username TEXT NOT NULL,
                        avatar TEXT NOT NULL,
                        last_seen TEXT NOT NULL)''')
            
            conn.commit()
            
            c.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = c.fetchall()
            logger.info(f"Database tables: {tables}")
        finally:
            if 'conn' in locals():
                conn.close()

    def get_user_profile(self, ip_address: str) -> dict:
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute('SELECT username, avatar FROM users WHERE ip_address = ?', (ip_address,))
            result = c.fetchone()
            
            if result:
                return {
                    'username': result[0],
                    'avatar': result[1]
                }
            return None
        except sqlite3.Error as e:
            logger.error(f"Error getting user profile: {e}")
            return None
        finally:
            if 'conn' in locals():
                conn.close()

    def save_user_profile(self, ip_address: str, username: str, avatar: str):
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            c.execute('''INSERT OR REPLACE INTO users 
                        (ip_address, username, avatar, last_seen)
                        VALUES (?, ?, ?, ?)''',
                     (ip_address, username, avatar, datetime.now().isoformat()))
            conn.commit()
        except sqlite3.Error as e:
            logger.error(f"Error saving user profile: {e}")
            raise
        finally:
            if 'conn' in locals():
                conn.close()

    def save_message(self, username: str, avatar: str, content: str, ip_address: str) -> str:
        try:
            conn = sqlite3.connect(self.db_path)
            timestamp = datetime.now().isoformat()
            c = conn.cursor()
            logger.info(f"Saving message with IP {ip_address}")
            
            c.execute('''INSERT INTO messages 
                        (username, avatar, content, timestamp, ip_address)
                        VALUES (?, ?, ?, ?, ?)''',
                    (username, avatar, content, timestamp, ip_address))
            conn.commit()
            return timestamp
        finally:
            conn.close()

    def get_recent_messages(self, limit: int = 50, current_ip: str = None) -> list:
        try:
            conn = sqlite3.connect(self.db_path)
            c = conn.cursor()
            logger.info(f"Loading messages for IP {current_ip}")
            
            c.execute('''SELECT username, avatar, content, timestamp, ip_address 
                        FROM messages ORDER BY id DESC LIMIT ?''', (limit,))
                        
            messages = []
            for row in c.fetchall():
                stored_ip = str(row[4]).strip()
                current_ip = str(current_ip).strip()
                
                messages.append({
                    "type": "message",
                    "username": row[0],
                    "avatar": row[1],
                    "content": row[2],
                    "timestamp": row[3],
                    "is_own_message": stored_ip == current_ip,
                    "ip_address": stored_ip
                })
                
            logger.info(f"Loaded {len(messages)} messages")
            return messages[::-1]
        finally:
            conn.close()

class ConnectionManager:
    def __init__(self):
        self.active_connections = {}
        self.db = DatabaseManager()

    async def connect(self, websocket: web.WebSocketResponse, client_id: str, ip_address: str):
        try:
            profile = self.db.get_user_profile(ip_address)
            
            if profile:
                username = profile['username']
                avatar = profile['avatar']
            else:
                username = f"User-{client_id[:8]}"
                avatar = f"https://api.dicebear.com/6.x/avataaars/svg?seed={client_id}"
                try:
                    self.db.save_user_profile(ip_address, username, avatar)
                except Exception as e:
                    logger.error(f"Error saving new user profile: {e}")

            self.active_connections[client_id] = {
                'websocket': websocket,
                'username': username,
                'avatar': avatar,
                'ip_address': ip_address,
                'last_pong': datetime.now()
            }
            logger.info(f"Client {client_id} connected from {ip_address}")
        except Exception as e:
            logger.error(f"Error in connect: {e}")
            raise

    async def disconnect(self, client_id: str, broadcast_message=True):
        if client_id in self.active_connections:
            if broadcast_message:
                disconnected_user = self.active_connections[client_id]['username']
                await self._broadcast_to_others({
                    'type': 'user_left',
                    'username': disconnected_user
                }, client_id)
            
            del self.active_connections[client_id]
            logger.info(f"Client {client_id} disconnected")

    async def update_profile(self, client_id: str, new_username: str, new_avatar: str):
        if client_id in self.active_connections:
            connection = self.active_connections[client_id]
            old_username = connection['username']
            
            connection['username'] = new_username
            connection['avatar'] = new_avatar
            
            try:
                self.db.save_user_profile(
                    connection['ip_address'],
                    new_username,
                    new_avatar
                )
            except Exception as e:
                logger.error(f"Error updating profile in database: {e}")
                return
            
            await self._broadcast_to_others({
                'type': 'user_updated',
                'old_username': old_username,
                'new_username': new_username
            }, client_id)
            
            await connection['websocket'].send_json({
                'type': 'profile_updated',
                'username': new_username,
                'avatar': new_avatar
            })
            
            logger.info(f"Client {client_id} updated profile: {new_username}")

    async def _broadcast_to_others(self, message: dict, sender_id: str = None):
        for client_id, client in list(self.active_connections.items()):
            if client_id != sender_id:
                try:
                    await client['websocket'].send_json(message)
                except Exception as e:
                    logger.error(f"Failed to send to client {client_id}: {e}")
                    await self.disconnect(client_id, broadcast_message=False)

    async def broadcast(self, message: dict, sender_id: str = None):
        sender_connection = self.active_connections.get(sender_id, {})
        sender_ip = sender_connection.get('ip_address')
        
        for client_id, client in list(self.active_connections.items()):
            try:
                message_copy = message.copy()
                message_copy['is_own_message'] = (client.get('ip_address') == sender_ip)
                await client['websocket'].send_json(message_copy)
            except Exception as e:
                logger.error(f"Failed to send to client {client_id}: {e}")
                await self.disconnect(client_id, broadcast_message=False)

    async def handle_ping(self, client_id: str):
        if client_id in self.active_connections:
            self.active_connections[client_id]['last_pong'] = datetime.now()

async def websocket_handler(request):
    ws = web.WebSocketResponse(heartbeat=30)
    await ws.prepare(request)
    
    client_id = request.match_info['client_id']
    manager = request.app['connection_manager']
    
    try:
        real_ip = request.headers.get('X-Forwarded-For', '').split(',')[0].strip() or \
                 request.headers.get('X-Real-IP', '') or \
                 request.remote or \
                 '127.0.0.1'
                 
        await manager.connect(ws, client_id, real_ip)
        
        recent_messages = manager.db.get_recent_messages(current_ip=real_ip)
        if recent_messages:
            for msg in recent_messages:
                msg['is_own_message'] = str(msg.get('ip_address')) == str(real_ip)
                await ws.send_json(msg)
        
        await manager._broadcast_to_others({
            'type': 'user_joined',
            'username': manager.active_connections[client_id]['username']
        }, client_id)

        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                try:
                    data = json.loads(msg.data)
                    
                    if data['type'] == 'ping':
                        await manager.handle_ping(client_id)
                        continue
                    
                    if data['type'] == 'update_profile':
                        await manager.update_profile(
                            client_id,
                            data['username'],
                            data['avatar']
                        )
                        continue
                        
                    if data['type'] == 'message':
                        content = data.get('content', '').strip()
                        if not content:
                            continue

                        timestamp = manager.db.save_message(
                            manager.active_connections[client_id]['username'],
                            manager.active_connections[client_id]['avatar'],
                            content,
                            real_ip
                        )
                        
                        await manager.broadcast({
                            'type': 'message',
                            'username': manager.active_connections[client_id]['username'],
                            'avatar': manager.active_connections[client_id]['avatar'],
                            'content': content,
                            'timestamp': timestamp
                        }, client_id)
                        
                except json.JSONDecodeError:
                    logger.warning(f"Invalid JSON from client {client_id}")
                except Exception as e:
                    logger.error(f"Message processing error: {e}")
                    
            elif msg.type == web.WSMsgType.ERROR:
                logger.error(f'WebSocket error: {ws.exception()}')
                
    except Exception as e:
        logger.error(f"WebSocket handler error: {e}")
    finally:
        await manager.disconnect(client_id)
        
    return ws

async def handle_index(request):
    index_path = Path(__file__).parent / 'index.html'
    if not index_path.exists():
        raise web.HTTPNotFound(text="index.html not found")
    return web.FileResponse(index_path)

async def init_app():
    app = web.Application(middlewares=[
        cors_middleware(
            origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )
    ])
    
    app['forwarded_allow_ips'] = '*'
    
    app['connection_manager'] = ConnectionManager()
    
    app.router.add_get('/', handle_index)
    app.router.add_get('/ws/{client_id}', websocket_handler)
    
    return app

def main():
    app = asyncio.run(init_app())
    port = int(os.environ.get('PORT', 8001))
    web.run_app(app, host='0.0.0.0', port=port)

if __name__ == "__main__":
    main()

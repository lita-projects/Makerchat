import asyncio
import json
import logging
import aiosqlite
from datetime import datetime
from pathlib import Path
import os
from aiohttp import web
from aiohttp_middlewares import cors_middleware
import aiofiles
import uuid

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DatabaseManager:
    def __init__(self):
        self.db_path = Path('data/chat.db')
        self.db_path.parent.mkdir(exist_ok=True)
        
    async def init_db(self):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute('''
                CREATE TABLE IF NOT EXISTS messages (
                    id TEXT PRIMARY KEY,
                    username TEXT,
                    avatar TEXT,
                    content TEXT,
                    timestamp TEXT,
                    ip_address TEXT
                )
            ''')
            await db.commit()

    async def save_message(self, msg_id: str, username: str, avatar: str, content: str, timestamp: str, ip_address: str):
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                'INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?)',
                (msg_id, username, avatar, content, timestamp, ip_address)
            )
            await db.commit()

    async def get_recent_messages(self, limit: int = 50):
        async with aiosqlite.connect(self.db_path) as db:
            async with db.execute(
                'SELECT * FROM messages ORDER BY timestamp DESC LIMIT ?',
                (limit,)
            ) as cursor:
                messages = await cursor.fetchall()
                return [
                    {
                        "id": msg[0],
                        "username": msg[1],
                        "avatar": msg[2],
                        "content": msg[3],
                        "timestamp": msg[4],
                        "ip_address": msg[5]
                    }
                    for msg in reversed(messages)
                ]

class FileStorageManager:
    def __init__(self):
        self.base_path = Path(os.environ.get('STORAGE_PATH', 'data'))
        self.users_file = self.base_path / 'users.json'
        self.db = DatabaseManager()
        self._init_storage()

    def _init_storage(self):
        self.base_path.mkdir(exist_ok=True)
        if not self.users_file.exists():
            self._write_json(self.users_file, {})

    async def init(self):
        await self.db.init_db()

    def _write_json(self, file_path: Path, data: dict):
        with open(file_path, 'w') as f:
            json.dump(data, f, indent=2)

    async def _async_read_json(self, file_path: Path) -> dict:
        try:
            async with aiofiles.open(file_path, 'r') as f:
                content = await f.read()
                return json.loads(content)
        except FileNotFoundError:
            return {}
        except json.JSONDecodeError:
            logger.error(f"Error reading {file_path}. Initializing as empty.")
            return {}

    async def _async_write_json(self, file_path: Path, data: dict):
        async with aiofiles.open(file_path, 'w') as f:
            await f.write(json.dumps(data, indent=2))

    async def get_user_profile(self, ip_address: str) -> dict:
        users = await self._async_read_json(self.users_file)
        return users.get(ip_address)

    async def save_user_profile(self, ip_address: str, username: str, avatar: str):
        users = await self._async_read_json(self.users_file)
        users[ip_address] = {
            'username': username,
            'avatar': avatar,
            'last_seen': datetime.now().isoformat()
        }
        await self._async_write_json(self.users_file, users)

    async def save_message(self, username: str, avatar: str, content: str, ip_address: str) -> str:
        timestamp = datetime.now().isoformat()
        msg_id = str(uuid.uuid4())
        await self.db.save_message(msg_id, username, avatar, content, timestamp, ip_address)
        return timestamp

    async def get_recent_messages(self, limit: int = 50, current_ip: str = None) -> list:
        messages = await self.db.get_recent_messages(limit)
        return [{
            "type": "message",
            "username": msg['username'],
            "avatar": msg['avatar'],
            "content": msg['content'],
            "timestamp": msg['timestamp'],
            "is_own_message": str(msg['ip_address']) == str(current_ip)
        } for msg in messages]

class ConnectionManager:
    def __init__(self):
        self.active_connections = {}
        self.db = FileStorageManager()

    async def connect(self, websocket: web.WebSocketResponse, client_id: str, ip_address: str):
        try:
            profile = await self.db.get_user_profile(ip_address)
            
            if profile:
                username = profile['username']
                avatar = profile['avatar']
            else:
                username = f"User-{client_id[:8]}"
                avatar = f"https://api.dicebear.com/6.x/avataaars/svg?seed={client_id}"
                try:
                    await self.db.save_user_profile(ip_address, username, avatar)
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
                await self.db.save_user_profile(
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
        
        recent_messages = await manager.db.get_recent_messages(current_ip=real_ip)
        if recent_messages:
            for msg in recent_messages:
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

                        timestamp = await manager.db.save_message(
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
    
    storage_path = Path(os.environ.get('STORAGE_PATH', 'data'))
    storage_path.mkdir(exist_ok=True)
    
    manager = ConnectionManager()
    await manager.db.init()
    app['connection_manager'] = manager
    
    app.router.add_get('/', handle_index)
    app.router.add_get('/ws/{client_id}', websocket_handler)
    
    return app

def main():
    port = int(os.environ.get('PORT', 8001))
    app = asyncio.run(init_app())
    web.run_app(app, host='0.0.0.0', port=port)

if __name__ == "__main__":
    main()

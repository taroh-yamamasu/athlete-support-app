"""
Pirates Trainer App - Database Manager
Handles all PostgreSQL interactions.
"""
import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from werkzeug.security import generate_password_hash
from typing import List, Dict, Any, Optional, Union

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DB_URL = os.environ.get('DATABASE_URL', None)

class DatabaseManager:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if self._initialized: return
        if DB_URL:
            self._initialize_db()
            self._initialized = True

    def _connect(self):
        return psycopg2.connect(DB_URL, sslmode='require')

    def _initialize_db(self):
        try:
            with self._connect() as conn:
                with conn.cursor() as c:
                    c.execute("CREATE TABLE IF NOT EXISTS USER_MASTER (user_id SERIAL PRIMARY KEY, username TEXT NOT NULL UNIQUE, password_hash TEXT NOT NULL, is_admin INTEGER NOT NULL DEFAULT 0)")
                    c.execute("CREATE TABLE IF NOT EXISTS PLAYER_MASTER (player_id SERIAL PRIMARY KEY, player_name TEXT NOT NULL UNIQUE)")
                    c.execute("CREATE TABLE IF NOT EXISTS KARTY_DATA (karte_id SERIAL PRIMARY KEY, player_id INTEGER, date TEXT NOT NULL, tr TEXT, time_loss TEXT, time_loss_category TEXT, diagnosis_flag INTEGER DEFAULT 0, s_content TEXT, o_content TEXT, a_content TEXT, p_content TEXT, activity TEXT, timing TEXT, age TEXT, status TEXT, mechanism TEXT, injury_type TEXT, injury_site TEXT, position TEXT, onset_style TEXT, FOREIGN KEY (player_id) REFERENCES PLAYER_MASTER(player_id))")
                    c.execute("SELECT user_id FROM USER_MASTER WHERE username = 'admin'")
                    if c.fetchone() is None:
                        c.execute("INSERT INTO USER_MASTER (username, password_hash, is_admin) VALUES (%s, %s, %s)", ('admin', generate_password_hash('password'), 1))
                conn.commit()
        except Exception as e: logger.error(f"DB初期化エラー: {e}")

    def migrate_schema(self) -> bool:
        statements = [
            "ALTER TABLE KARTY_DATA ADD COLUMN IF NOT EXISTS report_flag INTEGER DEFAULT 0",
            "ALTER TABLE KARTY_DATA ADD COLUMN IF NOT EXISTS injury_name TEXT",
            "ALTER TABLE KARTY_DATA ADD COLUMN IF NOT EXISTS participation_status TEXT",
            "ALTER TABLE KARTY_DATA ADD COLUMN IF NOT EXISTS return_est TEXT",
            "ALTER TABLE KARTY_DATA ADD COLUMN IF NOT EXISTS progress_note TEXT"
        ]
        try:
            with self._connect() as conn:
                with conn.cursor() as c:
                    for sql in statements: c.execute(sql)
                conn.commit()
            return True
        except Exception as e:
            logger.error(f"スキーマ更新エラー: {e}")
            return False

    def _execute(self, query: str, params: tuple = None, fetch_all: bool = False):
        try:
            with self._connect() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute(query, params or ())
                    if fetch_all: return [dict(row) for row in c.fetchall()]
                    res = c.fetchone()
                    return dict(res) if res else None
        except Exception as e:
            logger.error(f"SQL実行エラー: {e}")
            return [] if fetch_all else None

    def _sanitize_values(self, data: Dict[str, Any]) -> List[Any]:
        return [v if v != '' else None for v in data.values()]

    def get_users(self): return self._execute("SELECT user_id, username, is_admin FROM USER_MASTER ORDER BY user_id", fetch_all=True)
    def add_user(self, un, pw, ad):
        try:
            with self._connect() as conn:
                with conn.cursor() as c: c.execute("INSERT INTO USER_MASTER (username, password_hash, is_admin) VALUES (%s, %s, %s)", (un, generate_password_hash(pw), ad))
                conn.commit()
            return True
        except: return False
    def delete_user(self, uid):
        with self._connect() as conn:
            with conn.cursor() as c: c.execute("DELETE FROM USER_MASTER WHERE user_id = %s", (uid,))
            conn.commit()

    def get_players(self): return self._execute('SELECT player_id, player_name FROM PLAYER_MASTER ORDER BY player_name', fetch_all=True)
    def get_player(self, pid): return self._execute("SELECT player_id, player_name FROM PLAYER_MASTER WHERE player_id = %s", (pid,))
    def add_player(self, name):
        try:
            with self._connect() as conn:
                with conn.cursor() as c: c.execute("INSERT INTO PLAYER_MASTER (player_name) VALUES (%s)", (name,))
                conn.commit()
            return True
        except: return False
    def update_player_name(self, pid, name):
        try:
            with self._connect() as conn:
                with conn.cursor() as c: c.execute("UPDATE PLAYER_MASTER SET player_name = %s WHERE player_id = %s", (name, pid))
                conn.commit()
            return True
        except: return False
    def delete_player(self, pid):
        with self._connect() as conn:
            with conn.cursor() as c:
                c.execute("DELETE FROM KARTY_DATA WHERE player_id = %s", (pid,))
                c.execute("DELETE FROM PLAYER_MASTER WHERE player_id = %s", (pid,))
            conn.commit()

    def search_karty(self, filters):
        query = "SELECT k.karte_id, k.date, p.player_name, k.tr, k.a_content, k.time_loss_category, k.diagnosis_flag, k.report_flag FROM KARTY_DATA k LEFT JOIN PLAYER_MASTER p ON k.player_id = p.player_id WHERE 1=1"
        params = []
        if filters.get('player_id'): query += " AND k.player_id = %s"; params.append(filters['player_id'])
        if filters.get('start_date'): query += " AND k.date >= %s"; params.append(filters['start_date'])
        if filters.get('end_date'): query += " AND k.date <= %s"; params.append(filters['end_date'])
        if filters.get('time_loss_category'):
            if filters['time_loss_category'] == 'TIME_LOSS_ONLY': query += " AND (k.time_loss_category = 'TIME LOSS' OR k.time_loss_category = 'RETURN TO PLAY')"
            elif filters['time_loss_category'] != 'ALL': query += " AND k.time_loss_category = %s"; params.append(filters['time_loss_category'])
        if filters.get('keyword'):
            kw = f"%{filters['keyword']}%"
            query += " AND (k.s_content LIKE %s OR k.o_content LIKE %s OR k.a_content LIKE %s OR k.p_content LIKE %s OR k.tr LIKE %s)"
            params.extend([kw, kw, kw, kw, kw])
        query += " ORDER BY k.date DESC"
        return self._execute(query, params, fetch_all=True)

    def create_karte(self, data: Dict):
        cols = ', '.join(data.keys())
        plds = ', '.join(['%s'] * len(data))
        sql = f"INSERT INTO KARTY_DATA ({cols}) VALUES ({plds})"
        with self._connect() as conn:
            with conn.cursor() as c: c.execute(sql, self._sanitize_values(data))
            conn.commit()

    def update_karte(self, kid, data: Dict):
        set_c = ', '.join([f"{key} = %s" for key in data.keys()])
        sql = f"UPDATE KARTY_DATA SET {set_c} WHERE karte_id = %s"
        with self._connect() as conn:
            with conn.cursor() as c: c.execute(sql, self._sanitize_values(data) + [kid])
            conn.commit()

    def get_karte(self, kid): return self._execute("SELECT k.*, p.player_name FROM KARTY_DATA k LEFT JOIN PLAYER_MASTER p ON k.player_id=p.player_id WHERE k.karte_id = %s", (kid,))
    def get_latest_karte_by_player(self, pid): return self._execute("SELECT * FROM KARTY_DATA WHERE player_id = %s ORDER BY date DESC, karte_id DESC LIMIT 1", (pid,))
    def delete_karte(self, kid):
        with self._connect() as conn:
            with conn.cursor() as c: c.execute("DELETE FROM KARTY_DATA WHERE karte_id = %s", (kid,))
            conn.commit()

    def get_all_time_loss_categories(self): return self._execute("SELECT time_loss_category, COUNT(karte_id) as count FROM KARTY_DATA WHERE time_loss_category IN ('TIME LOSS', 'NEW/RE-INJURY', 'RETURN TO PLAY') GROUP BY time_loss_category", fetch_all=True)
    def get_injury_report_data(self): return self._execute("SELECT time_loss_category, injury_site, injury_type, COUNT(karte_id) as count FROM KARTY_DATA WHERE time_loss_category IN ('TIME LOSS', 'NEW/RE-INJURY', 'RETURN TO PLAY') GROUP BY time_loss_category, injury_site, injury_type HAVING injury_site IS NOT NULL AND injury_site != ''", fetch_all=True)
    def get_player_summary_data(self, pid):
        return {
            'stats': self._execute("SELECT COUNT(karte_id) as total_kartes FROM KARTY_DATA WHERE player_id = %s", (pid,)),
            'time_loss_stats': self._execute("SELECT COUNT(CASE WHEN time_loss_category = 'TIME LOSS' THEN 1 END) as tl_count, COUNT(CASE WHEN time_loss_category = 'RETURN TO PLAY' THEN 1 END) as rtp_count FROM KARTY_DATA WHERE player_id = %s", (pid,)),
            'history': self._execute("SELECT date, injury_site, injury_type, a_content, time_loss_category FROM KARTY_DATA WHERE player_id = %s ORDER BY date DESC LIMIT 10", (pid,), fetch_all=True)
        }
    def get_coach_reports(self): return self._execute("SELECT DISTINCT ON (k.player_id) k.*, p.player_name FROM KARTY_DATA k LEFT JOIN PLAYER_MASTER p ON k.player_id = p.player_id WHERE k.report_flag = 1 ORDER BY k.player_id, k.date DESC, k.karte_id DESC", fetch_all=True)
    def get_latest_injury_date(self, pid, dt):
        res = self._execute("SELECT date FROM KARTY_DATA WHERE player_id = %s AND time_loss_category = 'NEW/RE-INJURY' AND date <= %s ORDER BY date DESC LIMIT 1", (pid, dt))
        return res['date'] if res else None

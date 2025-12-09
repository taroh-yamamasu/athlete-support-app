import psycopg2
from psycopg2.extras import RealDictCursor
from werkzeug.security import generate_password_hash
import os

# Neonから取得したConnection Stringをここに貼り付けます
# 本番公開時は環境変数(os.environ)から読み込むように変更するのが安全です
DB_URL = os.environ.get('DATABASE_URL',None)
if DB_URL is None:
    raise ValueError("DATABASE_URL environment variable not set. This is required for deployment.")

class DatabaseManager:
    _instance = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            try:
                cls._instance._initialize_db()
            except Exception as e:
                print(f"DB初期化エラー: {e}")
        return cls._instance

    def _connect(self):
        # PostgreSQLに接続
        return psycopg2.connect(DB_URL)

    def _initialize_db(self):
        with self._connect() as conn:
            with conn.cursor() as c:
                # ユーザーテーブル (PostgreSQL用の型に変更)
                c.execute("""
                    CREATE TABLE IF NOT EXISTS USER_MASTER (
                        user_id SERIAL PRIMARY KEY,
                        username TEXT NOT NULL UNIQUE,
                        password_hash TEXT NOT NULL,
                        is_admin INTEGER NOT NULL DEFAULT 0
                    )
                """)
                
                c.execute("""
                    CREATE TABLE IF NOT EXISTS PLAYER_MASTER (
                        player_id SERIAL PRIMARY KEY,
                        player_name TEXT NOT NULL UNIQUE
                    )
                """)

                c.execute("""
                    CREATE TABLE IF NOT EXISTS KARTY_DATA (
                        karte_id SERIAL PRIMARY KEY,
                        player_id INTEGER NOT NULL,
                        date TEXT NOT NULL,
                        tr TEXT,
                        time_loss TEXT,
                        time_loss_category TEXT,
                        diagnosis_flag INTEGER DEFAULT 0,
                        s_content TEXT,
                        o_content TEXT,
                        a_content TEXT,
                        p_content TEXT,
                        activity TEXT,
                        timing TEXT,
                        age TEXT,
                        status TEXT,
                        mechanism TEXT,
                        injury_type TEXT,
                        injury_site TEXT,
                        position TEXT,
                        onset_style TEXT,
                        FOREIGN KEY (player_id) REFERENCES PLAYER_MASTER(player_id)
                    )
                """)
                
                # 初期ユーザー登録
                c.execute("SELECT user_id FROM USER_MASTER WHERE username = 'admin'")
                if c.fetchone() is None:
                    p_hash = generate_password_hash('password')
                    c.execute("INSERT INTO USER_MASTER (username, password_hash, is_admin) VALUES (%s, %s, %s)", 
                              ('admin', p_hash, 1))
            conn.commit()

    # --- 共通実行メソッド (PostgreSQL版) ---
    def _execute(self, query, params=None, fetch_all=False):
        with self._connect() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as c:
                c.execute(query, params or ())
                if fetch_all:
                    return [dict(row) for row in c.fetchall()]
                try:
                    res = c.fetchone()
                    return dict(res) if res else None
                except psycopg2.ProgrammingError:
                    return None

    def get_users(self):
        return self._execute("SELECT user_id, username, is_admin FROM USER_MASTER ORDER BY user_id", fetch_all=True)

    def add_user(self, username, password, is_admin=0):
        p_hash = generate_password_hash(password)
        try:
            with self._connect() as conn:
                with conn.cursor() as c:
                    c.execute("INSERT INTO USER_MASTER (username, password_hash, is_admin) VALUES (%s, %s, %s)",
                              (username, p_hash, is_admin))
                conn.commit()
            return True
        except psycopg2.errors.UniqueViolation:
            return False

    def delete_user(self, user_id):
        with self._connect() as conn:
            with conn.cursor() as c:
                c.execute("DELETE FROM USER_MASTER WHERE user_id = %s", (user_id,))
            conn.commit()

    def get_players(self):
        return self._execute('SELECT player_id, player_name FROM PLAYER_MASTER ORDER BY player_name', fetch_all=True)

    def get_player(self, player_id):
        return self._execute("SELECT player_id, player_name FROM PLAYER_MASTER WHERE player_id = %s", (player_id,))

    def add_player(self, name):
        try:
            with self._connect() as conn:
                with conn.cursor() as c:
                    c.execute("INSERT INTO PLAYER_MASTER (player_name) VALUES (%s)", (name,))
                conn.commit()
        except psycopg2.errors.UniqueViolation:
            pass

    def update_player_name(self, player_id, new_name):
        try:
            with self._connect() as conn:
                with conn.cursor() as c:
                    c.execute("UPDATE PLAYER_MASTER SET player_name = %s WHERE player_id = %s", (new_name, player_id))
                conn.commit()
            return True
        except psycopg2.errors.UniqueViolation:
            return False

    def delete_player(self, player_id):
        with self._connect() as conn:
            with conn.cursor() as c:
                c.execute("DELETE FROM KARTY_DATA WHERE player_id = %s", (player_id,))
                c.execute("DELETE FROM PLAYER_MASTER WHERE player_id = %s", (player_id,))
            conn.commit()

    def search_karty(self, filters):
        query = """
            SELECT k.karte_id, k.date, p.player_name, k.tr, k.a_content, k.time_loss_category, k.diagnosis_flag
            FROM KARTY_DATA k
            JOIN PLAYER_MASTER p ON k.player_id = p.player_id
            WHERE 1=1
        """
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

    def create_karte(self, data):
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        sql = f"INSERT INTO KARTY_DATA ({columns}) VALUES ({placeholders})"
        with self._connect() as conn:
            with conn.cursor() as c:
                c.execute(sql, list(data.values()))
            conn.commit()

    def update_karte(self, karte_id, data):
        set_clause = ', '.join([f"{key} = %s" for key in data.keys()])
        sql = f"UPDATE KARTY_DATA SET {set_clause} WHERE karte_id = %s"
        params = list(data.values()) + [karte_id]
        with self._connect() as conn:
            with conn.cursor() as c:
                c.execute(sql, params)
            conn.commit()

    def get_karte(self, karte_id):
        q = "SELECT k.*, p.player_name FROM KARTY_DATA k JOIN PLAYER_MASTER p ON k.player_id=p.player_id WHERE k.karte_id = %s"
        return self._execute(q, (karte_id,))
    
    def get_latest_karte_by_player(self, player_id):
        q = "SELECT * FROM KARTY_DATA WHERE player_id = %s ORDER BY date DESC, karte_id DESC LIMIT 1"
        return self._execute(q, (player_id,))

    def delete_karte(self, karte_id):
        with self._connect() as conn:
            with conn.cursor() as c:
                c.execute("DELETE FROM KARTY_DATA WHERE karte_id = %s", (karte_id,))
            conn.commit()
            
    def get_all_time_loss_categories(self):
        q = "SELECT time_loss_category, COUNT(karte_id) as count FROM KARTY_DATA WHERE time_loss_category IN ('TIME LOSS', 'NEW/RE-INJURY', 'RETURN TO PLAY') GROUP BY time_loss_category"
        return self._execute(q, fetch_all=True)

    def get_injury_report_data(self):
        q = "SELECT time_loss_category, injury_site, injury_type, COUNT(karte_id) as count FROM KARTY_DATA WHERE time_loss_category IN ('TIME LOSS', 'NEW/RE-INJURY', 'RETURN TO PLAY') GROUP BY time_loss_category, injury_site, injury_type HAVING injury_site IS NOT NULL AND injury_site != ''"
        return self._execute(q, fetch_all=True)

    def get_player_summary_data(self, player_id):
        stats = self._execute("SELECT COUNT(karte_id) as total_kartes FROM KARTY_DATA WHERE player_id = %s", (player_id,))
        tl_stats = self._execute("SELECT COUNT(CASE WHEN time_loss_category = 'TIME LOSS' THEN 1 END) as tl_count, COUNT(CASE WHEN time_loss_category = 'RETURN TO PLAY' THEN 1 END) as rtp_count FROM KARTY_DATA WHERE player_id = %s", (player_id,))
        history = self._execute("SELECT date, injury_site, injury_type, a_content, time_loss_category FROM KARTY_DATA WHERE player_id = %s ORDER BY date DESC LIMIT 10", (player_id,), fetch_all=True)
        return {'stats': stats, 'time_loss_stats': tl_stats, 'history': history}

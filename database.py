"""
Pirates Trainer App - Database Manager
Handles all PostgreSQL interactions with robustness and type safety.
"""
import os
import logging
import psycopg2
from psycopg2.extras import RealDictCursor
from werkzeug.security import generate_password_hash
from typing import List, Dict, Any, Optional, Union

# ロギング設定 (printの代わりにログ出力を使用)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# DB接続設定
DB_URL = os.environ.get('DATABASE_URL', None)

class DatabaseManager:
    _instance = None
    
    def __new__(cls):
        """シングルトンパターンの実装（常に同じインスタンスを返す）"""
        if cls._instance is None:
            cls._instance = super(DatabaseManager, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        """初期化処理（一度だけ実行されるように制御）"""
        if self._initialized:
            return
            
        if DB_URL:
            self._initialize_db()
            self._initialized = True
        else:
            logger.warning("DATABASE_URLが設定されていません。データベース接続は機能しません。")

    def _connect(self):
        """データベース接続を作成して返す"""
        try:
            return psycopg2.connect(DB_URL, sslmode='require')
        except psycopg2.Error as e:
            logger.error(f"データベース接続エラー: {e}")
            raise

    def _initialize_db(self):
        """テーブル作成と初期データの投入"""
        try:
            with self._connect() as conn:
                with conn.cursor() as c:
                    # ユーザーマスタ
                    c.execute("""
                        CREATE TABLE IF NOT EXISTS USER_MASTER (
                            user_id SERIAL PRIMARY KEY,
                            username TEXT NOT NULL UNIQUE,
                            password_hash TEXT NOT NULL,
                            is_admin INTEGER NOT NULL DEFAULT 0
                        )
                    """)
                    # 選手マスタ
                    c.execute("""
                        CREATE TABLE IF NOT EXISTS PLAYER_MASTER (
                            player_id SERIAL PRIMARY KEY,
                            player_name TEXT NOT NULL UNIQUE
                        )
                    """)
                    # カルテデータ
                    c.execute("""
                        CREATE TABLE IF NOT EXISTS KARTY_DATA (
                            karte_id SERIAL PRIMARY KEY,
                            player_id INTEGER,
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
                    
                    # 初期ユーザー登録 (admin)
                    c.execute("SELECT user_id FROM USER_MASTER WHERE username = 'admin'")
                    if c.fetchone() is None:
                        p_hash = generate_password_hash('password')
                        c.execute("INSERT INTO USER_MASTER (username, password_hash, is_admin) VALUES (%s, %s, %s)", 
                                  ('admin', p_hash, 1))
                        logger.info("初期管理者ユーザー(admin)を作成しました。")
                conn.commit()
        except Exception as e:
            logger.error(f"テーブル初期化エラー: {e}")

    def migrate_schema(self) -> bool:
        """既存のテーブルに新機能用カラムを追加する（マイグレーション）"""
        alter_statements = [
            "ALTER TABLE KARTY_DATA ADD COLUMN IF NOT EXISTS report_flag INTEGER DEFAULT 0",
            "ALTER TABLE KARTY_DATA ADD COLUMN IF NOT EXISTS injury_name TEXT",
            "ALTER TABLE KARTY_DATA ADD COLUMN IF NOT EXISTS participation_status TEXT",
            "ALTER TABLE KARTY_DATA ADD COLUMN IF NOT EXISTS return_est TEXT",
            "ALTER TABLE KARTY_DATA ADD COLUMN IF NOT EXISTS progress_note TEXT"
        ]
        try:
            with self._connect() as conn:
                with conn.cursor() as c:
                    for sql in alter_statements:
                        c.execute(sql)
                conn.commit()
            logger.info("スキーマ更新(migrate_schema)が完了しました。")
            return True
        except Exception as e:
            logger.error(f"スキーマ更新エラー: {e}")
            return False

    def _execute(self, query: str, params: tuple = None, fetch_all: bool = False) -> Union[Dict, List[Dict], None]:
        """SQL実行の共通ラッパー関数"""
        try:
            with self._connect() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as c:
                    c.execute(query, params or ())
                    if fetch_all:
                        return [dict(row) for row in c.fetchall()]
                    
                    try:
                        # fetchoneの結果があればdictに変換、なければNone
                        res = c.fetchone()
                        return dict(res) if res else None
                    except psycopg2.ProgrammingError:
                        # INSERT/UPDATE/DELETEなどで結果がない場合
                        return None
        except Exception as e:
            logger.error(f"SQL実行エラー: {e} | Query: {query}")
            return [] if fetch_all else None

    # --- ユーティリティ ---
    def _sanitize_values(self, data: Dict[str, Any]) -> List[Any]:
        """辞書の値から空文字をNoneに変換してリスト化する（DRY対応）"""
        return [v if v != '' else None for v in data.values()]

    # --- ユーザー関連 ---
    def get_users(self) -> List[Dict]:
        return self._execute("SELECT user_id, username, is_admin FROM USER_MASTER ORDER BY user_id", fetch_all=True)

    def add_user(self, username, password, is_admin=0) -> bool:
        p_hash = generate_password_hash(password)
        try:
            with self._connect() as conn:
                with conn.cursor() as c:
                    c.execute("INSERT INTO USER_MASTER (username, password_hash, is_admin) VALUES (%s, %s, %s)",
                              (username, p_hash, is_admin))
                conn.commit()
            return True
        except psycopg2.errors.UniqueViolation:
            logger.warning(f"ユーザー登録失敗: {username} は既に存在します。")
            return False

    def delete_user(self, user_id):
        with self._connect() as conn:
            with conn.cursor() as c:
                c.execute("DELETE FROM USER_MASTER WHERE user_id = %s", (user_id,))
            conn.commit()

    # --- 選手マスタ関連 ---
    def get_players(self) -> List[Dict]:
        return self._execute('SELECT player_id, player_name FROM PLAYER_MASTER ORDER BY player_name', fetch_all=True)

    def get_player(self, player_id) -> Dict:
        return self._execute("SELECT player_id, player_name FROM PLAYER_MASTER WHERE player_id = %s", (player_id,))

    def add_player(self, name) -> bool:
        try:
            with self._connect() as conn:
                with conn.cursor() as c:
                    c.execute("INSERT INTO PLAYER_MASTER (player_name) VALUES (%s)", (name,))
                conn.commit()
            return True
        except psycopg2.errors.UniqueViolation:
            return False
        except Exception as e:
            logger.error(f"選手登録エラー: {e}")
            return False

    def update_player_name(self, player_id, new_name) -> bool:
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
                # 関連データの削除（外部キー制約がない場合の手動削除）
                c.execute("DELETE FROM KARTY_DATA WHERE player_id = %s", (player_id,))
                c.execute("DELETE FROM PLAYER_MASTER WHERE player_id = %s", (player_id,))
            conn.commit()

    # --- カルテ検索・操作 ---
    def search_karty(self, filters: Dict) -> List[Dict]:
        query = """
            SELECT k.karte_id, k.date, p.player_name, k.tr, k.a_content, k.time_loss_category, k.diagnosis_flag, k.report_flag
            FROM KARTY_DATA k
            LEFT JOIN PLAYER_MASTER p ON k.player_id = p.player_id
            WHERE 1=1
        """
        params = []
        if filters.get('player_id'): 
            query += " AND k.player_id = %s"
            params.append(filters['player_id'])
        if filters.get('start_date'): 
            query += " AND k.date >= %s"
            params.append(filters['start_date'])
        if filters.get('end_date'): 
            query += " AND k.date <= %s"
            params.append(filters['end_date'])
        if filters.get('time_loss_category'):
            if filters['time_loss_category'] == 'TIME_LOSS_ONLY': 
                query += " AND (k.time_loss_category = 'TIME LOSS' OR k.time_loss_category = 'RETURN TO PLAY')"
            elif filters['time_loss_category'] != 'ALL': 
                query += " AND k.time_loss_category = %s"
                params.append(filters['time_loss_category'])
        if filters.get('keyword'):
            kw = f"%{filters['keyword']}%"
            query += " AND (k.s_content LIKE %s OR k.o_content LIKE %s OR k.a_content LIKE %s OR k.p_content LIKE %s OR k.tr LIKE %s)"
            params.extend([kw, kw, kw, kw, kw])
        
        query += " ORDER BY k.date DESC"
        return self._execute(query, params, fetch_all=True)

    def create_karte(self, data: Dict):
        columns = ', '.join(data.keys())
        placeholders = ', '.join(['%s'] * len(data))
        sql = f"INSERT INTO KARTY_DATA ({columns}) VALUES ({placeholders})"
        
        values = self._sanitize_values(data)
                
        with self._connect() as conn:
            with conn.cursor() as c:
                c.execute(sql, values)
            conn.commit()

    def update_karte(self, karte_id, data: Dict):
        set_clause = ', '.join([f"{key} = %s" for key in data.keys()])
        sql = f"UPDATE KARTY_DATA SET {set_clause} WHERE karte_id = %s"
        
        values = self._sanitize_values(data)
        params = values + [karte_id]
        
        with self._connect() as conn:
            with conn.cursor() as c:
                c.execute(sql, params)
            conn.commit()

    def get_karte(self, karte_id) -> Dict:
        q = "SELECT k.*, p.player_name FROM KARTY_DATA k LEFT JOIN PLAYER_MASTER p ON k.player_id=p.player_id WHERE k.karte_id = %s"
        return self._execute(q, (karte_id,))
    
    def get_latest_karte_by_player(self, player_id) -> Dict:
        q = "SELECT * FROM KARTY_DATA WHERE player_id = %s ORDER BY date DESC, karte_id DESC LIMIT 1"
        return self._execute(q, (player_id,))

    def delete_karte(self, karte_id):
        with self._connect() as conn:
            with conn.cursor() as c:
                c.execute("DELETE FROM KARTY_DATA WHERE karte_id = %s", (karte_id,))
            conn.commit()
            
    # --- レポート・分析 ---
    def get_all_time_loss_categories(self) -> List[Dict]:
        q = "SELECT time_loss_category, COUNT(karte_id) as count FROM KARTY_DATA WHERE time_loss_category IN ('TIME LOSS', 'NEW/RE-INJURY', 'RETURN TO PLAY') GROUP BY time_loss_category"
        return self._execute(q, fetch_all=True)

    def get_injury_report_data(self) -> List[Dict]:
        q = "SELECT time_loss_category, injury_site, injury_type, COUNT(karte_id) as count FROM KARTY_DATA WHERE time_loss_category IN ('TIME LOSS', 'NEW/RE-INJURY', 'RETURN TO PLAY') GROUP BY time_loss_category, injury_site, injury_type HAVING injury_site IS NOT NULL AND injury_site != ''"
        return self._execute(q, fetch_all=True)

    def get_player_summary_data(self, player_id) -> Dict:
        stats = self._execute("SELECT COUNT(karte_id) as total_kartes FROM KARTY_DATA WHERE player_id = %s", (player_id,))
        tl_stats = self._execute("SELECT COUNT(CASE WHEN time_loss_category = 'TIME LOSS' THEN 1 END) as tl_count, COUNT(CASE WHEN time_loss_category = 'RETURN TO PLAY' THEN 1 END) as rtp_count FROM KARTY_DATA WHERE player_id = %s", (player_id,))
        history = self._execute("SELECT date, injury_site, injury_type, a_content, time_loss_category FROM KARTY_DATA WHERE player_id = %s ORDER BY date DESC LIMIT 10", (player_id,), fetch_all=True)
        return {'stats': stats, 'time_loss_stats': tl_stats, 'history': history}

    # --- コーチ用レポート取得 ---
    def get_coach_reports(self) -> List[Dict]:
        """各選手の最新の傷病報告カルテを取得"""
        query = """
            SELECT DISTINCT ON (k.player_id)
                k.karte_id, k.date, p.player_name, 
                k.injury_name, k.participation_status, k.return_est, k.progress_note,
                k.time_loss_category
            FROM KARTY_DATA k
            LEFT JOIN PLAYER_MASTER p ON k.player_id = p.player_id
            WHERE k.report_flag = 1
            ORDER BY k.player_id, k.date DESC, k.karte_id DESC
        """
        return self._execute(query, fetch_all=True)
    
    def get_latest_injury_date(self, player_id, current_date) -> Optional[str]:
        """指定した日付以前の、直近の受傷日(NEW/RE-INJURY)を取得"""
        query = """
            SELECT date FROM KARTY_DATA 
            WHERE player_id = %s 
            AND time_loss_category = 'NEW/RE-INJURY'
            AND date <= %s
            ORDER BY date DESC LIMIT 1
        """
        res = self._execute(query, (player_id, current_date))
        return res['date'] if res else None

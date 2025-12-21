"""
Pirates Trainer App - Main Application
Backend Logic (Flask)
Optimized for Robustness and Maintainability
"""
import os
import json
from datetime import datetime
from typing import Optional, Dict, Any, List

from flask import Flask, render_template, request, redirect, url_for, abort, flash, session
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import check_password_hash
from dotenv import load_dotenv

# データベースマネージャーのインポート
from database import DatabaseManager

# --- 設定と初期化 ---
load_dotenv() # .envファイルの読み込み

app = Flask(__name__)

# セキュリティキーの設定（本番環境では必ず強力なランダム文字列を環境変数に設定すること）
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'default_secret_key_for_local_test')

# コーチ用合言葉の設定
# 環境変数 'COACH_PASSWORD' があればそれを優先し、なければデフォルトの 'pirates' を使用
COACH_SHARED_PASSWORD = os.environ.get('COACH_PASSWORD', 'pirates')

# ログイン機能の初期化
login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'
login_manager.login_message = "このページにアクセスするにはログインが必要です。"
login_manager.login_message_category = "warning"

# データベース接続
db_manager = DatabaseManager()

# --- 定数定義（変更がある場合はここを修正するだけで全体に反映されます） ---
class Const:
    # 参加ステータス
    STATUS_IN = 'IN (参加)'
    STATUS_RESTRICTION = 'RESTRICTION (制限付)'
    STATUS_OUT = 'OUT (不参加)'
    STATUS_GTD = 'GTD (当日判断)'
    
    # タイムロス区分
    TL_NONE = 'NON TIME LOSS'
    TL_NEW = 'NEW/RE-INJURY'
    TL_LOSS = 'TIME LOSS'
    TL_RTP = 'RETURN TO PLAY'

# 選択肢リスト
PULLDOWN_OPTIONS = {
    "activity": {"label": "試合/練習", "options": ["試合", "練習"]},
    "timing": {"label": "タイミング", "options": ["1Q", "2Q", "3Q", "4Q", "walkthrough", "indy", "kick", "team", "scrimage", "strength training", "after training", "その他"]},
    "age": {"label": "年齢", "options": [str(i) for i in range(20, 46)]},
    "status": {"label": "状態", "options": ["新規", "再発", "悪化"]},
    "mechanism": {"label": "外力", "options": ["直達外力衝突", "介達外力衝突", "地面衝突", "ボール衝突", "非接触", "その他"]},
    "injury_type": {"label": "傷害の種類", "options": ["脳振盪/脳損傷", "脊髄損傷", "末梢神経損傷", "脱臼/亜脱臼", "骨折", "疲労性骨損傷", "骨挫傷", "無腐性壊死", "成長軟骨板損傷", "軟骨損傷（半月板含む）", "関節捻挫/靭帯損傷", "慢性の不安定症", "腱断裂", "腱障害", "筋痙攣", "肉離れ/筋損傷", "筋打撲傷", "筋コンパートメント症候群", "裂傷", "擦過傷", "打撲傷（表在性）", "関節炎", "滑液包炎", "滑膜炎", "血管損傷", "断端損傷", "内部臓器損傷", "その他", "不明/特定不能"]},
    "injury_site": {"label": "傷害の部位", "options": ["頭部", "顔面", "歯/口腔/顎", "頚部/頚椎", "肩", "上腕", "肘", "前腕", "手関節", "手", "胸部", "胸椎/上背部", "腰（仙椎/臀部含む）", "腹部", "股関節/鼠径部", "大腿前面", "大腿後面", "膝", "下腿/アキレス腱", "足関節", "足部", "不明/該当なし"]},
    "position": {"label": "ポジション", "options": ["QB", "OL", "WR", "RB", "TE", "DL", "LB", "CB", "SF", "NI", "K", "S", "その他"]},
    "onset_style": {"label": "発祥様式", "options": ["Acute sudden", "Repetitive sudden", "Repetitive gradual"]},
}

TIME_LOSS_OPTIONS = [Const.TL_NONE, Const.TL_NEW, Const.TL_LOSS, Const.TL_RTP]
PARTICIPATION_STATUS_OPTIONS = [Const.STATUS_IN, Const.STATUS_RESTRICTION, Const.STATUS_OUT, Const.STATUS_GTD]

# --- ユーザーモデル ---
class User(UserMixin):
    def __init__(self, user_id: int, username: str, is_admin: int):
        self.id = user_id
        self.username = username
        self.is_admin = is_admin

@login_manager.user_loader
def load_user(user_id):
    """Flask-Login用のユーザー読み込み関数"""
    user_data = db_manager._execute(
        "SELECT user_id, username, is_admin, password_hash FROM USER_MASTER WHERE user_id = %s", 
        (user_id,)
    )
    if user_data: 
        return User(user_data['user_id'], user_data['username'], user_data['is_admin'])
    return None

# --- ユーティリティ関数 ---

def prepare_karte_data(form_data: Dict[str, Any]) -> Dict[str, Any]:
    """フォームデータからカルテ保存用の辞書データを作成する"""
    player_id_value = form_data.get('player_id')
    
    # 基本データの構築
    data = {
        'date': form_data.get('date'),
        'player_id': player_id_value if player_id_value else None,
        'tr': form_data.get('tr', ''),
        'time_loss': form_data.get('time_loss', ''),
        'time_loss_category': form_data.get('time_loss_category'),
        'diagnosis_flag': 1 if form_data.get('diagnosis_flag') == 'on' else 0,
        's_content': form_data.get('s_content'),
        'o_content': form_data.get('o_content'),
        'a_content': form_data.get('a_content'),
        'p_content': form_data.get('p_content'),
        
        # コーチ共有用データ
        'report_flag': 1 if form_data.get('report_flag') == 'on' else 0,
        'injury_name': form_data.get('injury_name', ''),
        'participation_status': form_data.get('participation_status', ''),
        'return_est': form_data.get('return_est', ''),
        'progress_note': form_data.get('progress_note', '')
    }
    
    # プルダウン項目の安全な取得
    for key in PULLDOWN_OPTIONS.keys(): 
        value = form_data.get(key)
        data[key] = value if value is not None else '' 
        
    return data

# --- ルーティング設定 ---

@app.route('/sys_update_db')
@login_required
def sys_update_db():
    """データベーススキーマ更新用（管理者のみ）"""
    if not current_user.is_admin:
        return "管理者権限が必要です。", 403
    
    if db_manager.migrate_schema():
        return "データベースの更新（カラム追加）が完了しました！トップページに戻ってください。"
    else:
        return "データベース更新に失敗しました。ログを確認してください。"

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated:
        return redirect(url_for('index'))
        
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        user_data = db_manager._execute(
            "SELECT user_id, username, password_hash, is_admin FROM USER_MASTER WHERE username = %s", 
            (username,)
        )
        
        if user_data and check_password_hash(user_data['password_hash'], password):
            user = User(user_data['user_id'], user_data['username'], user_data['is_admin'])
            login_user(user) 
            flash(f'ようこそ、{user.username}さん', 'success')
            return redirect(url_for('index'))
        else:
            flash('ユーザー名またはパスワードが間違っています。', 'danger')
            
    return render_template('login.html')

@app.route('/logout')
def logout():
    logout_user()
    # コーチ用の認証セッションもクリアする（セキュリティ向上）
    session.pop('coach_authenticated', None)
    flash('ログアウトしました。', 'info')
    return redirect(url_for('login'))

@app.route('/report')
@login_required
def report():
    tl_counts = db_manager.get_all_time_loss_categories()
    report_data = db_manager.get_injury_report_data()
    
    # グラフ用データの整形
    site_summary = {}
    for item in report_data:
        site = item['injury_site']
        site_summary[site] = site_summary.get(site, 0) + item['count']
    
    sorted_sites = sorted(site_summary.items(), key=lambda x: x[1], reverse=True)
    chart_labels = [x[0] for x in sorted_sites]
    chart_values = [x[1] for x in sorted_sites]

    grouped_data = {}
    for item in report_data:
        cat = item['time_loss_category']
        if cat not in grouped_data: grouped_data[cat] = []
        grouped_data[cat].append(item)
        
    return render_template('report.html', 
                           tl_counts=tl_counts, 
                           grouped_data=grouped_data,
                           chart_labels=json.dumps(chart_labels),
                           chart_values=json.dumps(chart_values))

@app.route('/players/summary/<int:player_id>')
@login_required
def player_summary(player_id):
    player = db_manager.get_player(player_id)
    if not player:
        abort(404)
    summary = db_manager.get_player_summary_data(player_id)
    return render_template('player_summary.html', player=player, summary=summary)

# --- コーチ用ログイン & 閲覧画面 ---

@app.route('/coach_login', methods=['GET', 'POST'])
def coach_login():
    """コーチ共有画面への入り口（合言葉認証）"""
    if session.get('coach_authenticated'):
        return redirect(url_for('coach_view'))
        
    if request.method == 'POST':
        password = request.form.get('password')
        if password == COACH_SHARED_PASSWORD:
            session['coach_authenticated'] = True
            return redirect(url_for('coach_view'))
        else:
            flash('合言葉が違います。', 'danger')
            
    return render_template('coach_login.html')

@app.route('/coach_view')
def coach_view():
    """コーチ共有レポート表示（並び替え機能付き）"""
    # トレーナーログイン中、または合言葉認証済みの場合のみアクセス可
    if not current_user.is_authenticated and not session.get('coach_authenticated'):
        return redirect(url_for('coach_login'))
        
    reports = db_manager.get_coach_reports()
    
    # ★Proモード改善：並び替えロジックの強化
    # 優先順位マップ（数字が小さいほど上に表示）
    # Constクラスの値を使用することで、文字列変更時のバグを防ぐ
    priority_map = {
        Const.STATUS_OUT: 1,
        Const.STATUS_GTD: 2,
        Const.STATUS_RESTRICTION: 3,
        Const.STATUS_IN: 4
    }
    
    if reports:
        # get()メソッドの第二引数(99)で、想定外のステータスが来てもエラーにならず一番下に表示する
        reports.sort(key=lambda x: priority_map.get(x.get('participation_status'), 99))
    
    today = datetime.now().strftime('%Y-%m-%d')
    return render_template('coach_view.html', reports=reports, today=today)

# --- カルテ操作 (CRUD) ---

@app.route('/create_karte', methods=['GET', 'POST'])
@login_required 
def create_karte():
    player_list = db_manager.get_players()
    today = datetime.now().strftime('%Y-%m-%d')
    
    # コピー作成機能
    copied_karte = None
    copy_player_id = request.args.get('copy_player_id')
    copy_from_id = request.args.get('copy_from_id')

    if copy_from_id:
        copied_karte = db_manager.get_karte(copy_from_id)
    elif copy_player_id:
        copied_karte = db_manager.get_latest_karte_by_player(copy_player_id)
    
    if copied_karte:
        copied_karte['date'] = today
        copied_karte['karte_id'] = None # 新規作成扱いにするためID消去

    if request.method == 'POST':
        data = prepare_karte_data(request.form)
        
        if data['player_id'] is None:
            flash('エラー: 選手を選択してください。', 'danger')
            return render_template('karte_form.html', player_list=player_list, 
                                   PULLDOWN_OPTIONS=PULLDOWN_OPTIONS, 
                                   TIME_LOSS_OPTIONS=TIME_LOSS_OPTIONS, 
                                   PARTICIPATION_STATUS_OPTIONS=PARTICIPATION_STATUS_OPTIONS,
                                   karte=data, action='create', today=today)

        db_manager.create_karte(data)
        flash('カルテを作成しました。', 'success')
        return redirect(url_for('index'))
        
    return render_template('karte_form.html', player_list=player_list, 
                           PULLDOWN_OPTIONS=PULLDOWN_OPTIONS, 
                           TIME_LOSS_OPTIONS=TIME_LOSS_OPTIONS, 
                           PARTICIPATION_STATUS_OPTIONS=PARTICIPATION_STATUS_OPTIONS,
                           karte=copied_karte, action='create', today=today)

@app.route('/karte/<int:karte_id>', methods=['GET', 'POST']) 
@login_required 
def edit_karte(karte_id):
    karte = db_manager.get_karte(karte_id)
    if not karte:
        abort(404)
        
    player_list = db_manager.get_players()
    
    if request.method == 'POST':
        data = prepare_karte_data(request.form)
        db_manager.update_karte(karte_id, data)
        flash('カルテを更新しました。', 'success')
        return redirect(url_for('edit_karte', karte_id=karte_id))
        
    return render_template('karte_form.html', karte=karte, player_list=player_list, 
                           PULLDOWN_OPTIONS=PULLDOWN_OPTIONS, 
                           TIME_LOSS_OPTIONS=TIME_LOSS_OPTIONS, 
                           PARTICIPATION_STATUS_OPTIONS=PARTICIPATION_STATUS_OPTIONS,
                           action='edit')

@app.route('/')
@login_required 
def index():
    filters = {
        'player_id': request.args.get('player_id'),
        'start_date': request.args.get('start_date'),
        'end_date': request.args.get('end_date'),
        'keyword': request.args.get('keyword'),
        'time_loss_category': request.args.get('time_loss_category')
    }
    karte_data = db_manager.search_karty(filters)
    player_list = db_manager.get_players()
    return render_template('index.html', data=karte_data, player_list=player_list, filters=filters, TIME_LOSS_OPTIONS=TIME_LOSS_OPTIONS)

@app.route('/karte/delete/<int:karte_id>', methods=['POST'])
@login_required
def delete_karte(karte_id):
    db_manager.delete_karte(karte_id)
    flash('カルテを削除しました。', 'info')
    return redirect(url_for('index'))

# --- マスタ管理 ---

@app.route('/players', methods=['GET', 'POST'])
@login_required 
def player_master():
    if request.method == 'POST':
        name = request.form.get('player_name', '').strip()
        if name:
            if db_manager.add_player(name):
                flash(f'選手 {name} を登録しました', 'success')
            else:
                flash(f'エラー: 選手 {name} は既に登録されています', 'danger')
        return redirect(url_for('player_master'))
    players = db_manager.get_players()
    return render_template('player_master.html', players=players)

@app.route('/players/edit/<int:player_id>', methods=['POST'])
@login_required 
def edit_player(player_id):
    if request.form.get('action') == 'delete': 
        db_manager.delete_player(player_id)
        flash('選手を削除しました。', 'info')
    else:
        new_name = request.form.get('player_name', '').strip()
        if new_name: 
            db_manager.update_player_name(player_id, new_name)
            flash('選手名を更新しました。', 'success')
    return redirect(url_for('player_master'))

@app.route('/users', methods=['GET', 'POST'])
@login_required
def user_master():
    if not current_user.is_admin:
        flash('管理者権限が必要です', 'danger')
        return redirect(url_for('index'))
        
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'add':
            un = request.form.get('username')
            pw = request.form.get('password')
            ad = 1 if request.form.get('is_admin') else 0
            if db_manager.add_user(un, pw, ad): 
                flash(f'ユーザー {un} を追加しました', 'success')
            else: 
                flash('エラー: ユーザー名重複', 'danger')
        elif action == 'delete':
            uid = request.form.get('user_id')
            if int(uid) == current_user.id: 
                flash('自分自身の削除はできません。', 'warning')
            else: 
                db_manager.delete_user(uid)
                flash('ユーザーを削除しました。', 'info')
        return redirect(url_for('user_master'))
    users = db_manager.get_users()
    return render_template('user_master.html', users=users)

if __name__ == '__main__':
    app.run(debug=True)

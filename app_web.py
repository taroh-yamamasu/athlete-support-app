# app_web.py の完全版コード (load_user を %s に修正済み)

from flask import Flask, render_template, request, redirect, url_for, abort, flash
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import check_password_hash
from database import DatabaseManager  
from datetime import datetime
import json
import os 

app = Flask(__name__)  

app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'default_secret_key_for_local_test')

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'  

db_manager = DatabaseManager()

# 定数 (省略)
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
TIME_LOSS_OPTIONS = ['NON TIME LOSS', 'NEW/RE-INJURY', 'TIME LOSS', 'RETURN TO PLAY']

# --- Flask-Login User ---
class User(UserMixin):
    def __init__(self, user_id, username, is_admin):
        self.id = user_id
        self.username = username
        self.is_admin = is_admin

@login_manager.user_loader
def load_user(user_id):
    user_data = db_manager._execute(
        # ⭐ 修正箇所: ? から %s へ変更
        "SELECT user_id, username, is_admin, password_hash FROM USER_MASTER WHERE user_id = %s", 
        (user_id,)
    )
    if user_data: 
        return User(user_data['user_id'], user_data['username'], user_data['is_admin'])
    return None

# --- Routes ---
@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated: return redirect(url_for('index'))
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        # database.pyの_executeを利用し、辞書形式でデータを取得
        user_data = db_manager._execute("SELECT user_id, username, password_hash, is_admin FROM USER_MASTER WHERE username = %s", (username,))
        
        if user_data and check_password_hash(user_data['password_hash'], password):
            user = User(user_data['user_id'], user_data['username'], user_data['is_admin'])
            login_user(user) 
            return redirect(url_for('index'))
        else: flash('ログイン失敗', 'danger')
    return render_template('login.html')

@app.route('/logout')
def logout():
    logout_user()
    return redirect(url_for('login'))

@app.route('/report')
@login_required
def report():
    tl_counts = db_manager.get_all_time_loss_categories()
    report_data = db_manager.get_injury_report_data()
    
    chart_labels = []
    chart_values = []
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
    if not player: abort(404)
    summary = db_manager.get_player_summary_data(player_id)
    return render_template('player_summary.html', player=player, summary=summary)

# --- 共通カルテ操作ロジック ---
def prepare_karte_data(form_data):
    data = {
        'date': form_data.get('date'),
        'player_id': form_data.get('player_id'),
        'tr': form_data.get('tr', ''),
        'time_loss': form_data.get('time_loss', ''),
        'time_loss_category': form_data.get('time_loss_category'),
        'diagnosis_flag': 1 if form_data.get('diagnosis_flag') == 'on' else 0,
        's_content': form_data.get('s_content'),
        'o_content': form_data.get('o_content'),
        'a_content': form_data.get('a_content'),
        'p_content': form_data.get('p_content'),
    }
    for key in PULLDOWN_OPTIONS.keys(): data[key] = form_data.get(key)
    return data

@app.route('/create_karte', methods=['GET', 'POST'])
@login_required 
def create_karte():
    player_list = db_manager.get_players()
    today = datetime.now().strftime('%Y-%m-%d')
    copied_karte = None
    copy_player_id = request.args.get('copy_player_id')
    copy_from_id = request.args.get('copy_from_id')

    if copy_from_id:
        copied_karte = db_manager.get_karte(copy_from_id)
    elif copy_player_id:
        copied_karte = db_manager.get_latest_karte_by_player(copy_player_id)
    
    if copied_karte:
        copied_karte['date'] = today
        copied_karte['karte_id'] = None

    if request.method == 'POST':
        data = prepare_karte_data(request.form)
        db_manager.create_karte(data)
        flash('作成しました', 'success')
        return redirect(url_for('index'))
        
    return render_template('karte_form.html', player_list=player_list, PULLDOWN_OPTIONS=PULLDOWN_OPTIONS, 
                           TIME_LOSS_OPTIONS=TIME_LOSS_OPTIONS, karte=copied_karte, action='create', today=today)

@app.route('/karte/<int:karte_id>', methods=['GET', 'POST']) 
@login_required 
def edit_karte(karte_id):
    karte = db_manager.get_karte(karte_id)
    if not karte: abort(404)
    player_list = db_manager.get_players()
    if request.method == 'POST':
        data = prepare_karte_data(request.form)
        db_manager.update_karte(karte_id, data)
        flash('更新しました', 'success')
        return redirect(url_for('edit_karte', karte_id=karte_id))
    return render_template('karte_form.html', karte=karte, player_list=player_list, 
                           PULLDOWN_OPTIONS=PULLDOWN_OPTIONS, TIME_LOSS_OPTIONS=TIME_LOSS_OPTIONS, action='edit')

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
    flash('削除しました', 'info')
    return redirect(url_for('index'))

@app.route('/players', methods=['GET', 'POST'])
@login_required 
def player_master():
    if request.method == 'POST':
        name = request.form.get('player_name', '').strip()
        if name:
            if db_manager.add_player(name):
                flash(f'選手 {name} を登録しました', 'success')
            else:
                flash(f'エラー: 選手 {name} は既に登録されています (または登録に失敗しました)', 'danger')
        return redirect(url_for('player_master'))
    players = db_manager.get_players()
    return render_template('player_master.html', players=players)

@app.route('/players/edit/<int:player_id>', methods=['POST'])
@login_required 
def edit_player(player_id):
    if request.form.get('action') == 'delete': db_manager.delete_player(player_id)
    else:
        new_name = request.form.get('player_name', '').strip()
        if new_name: db_manager.update_player_name(player_id, new_name)
    return redirect(url_for('player_master'))

@app.route('/users', methods=['GET', 'POST'])
@login_required
def user_master():
    if not current_user.is_admin: flash('管理者権限が必要です', 'danger'); return redirect(url_for('index'))
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'add':
            un = request.form.get('username'); pw = request.form.get('password'); ad = 1 if request.form.get('is_admin') else 0
            if db_manager.add_user(un, pw, ad): flash(f'ユーザー {un} を追加しました', 'success')
            else: flash('エラー: ユーザー名重複', 'danger')
        elif action == 'delete':
            uid = request.form.get('user_id')
            if int(uid) == current_user.id: flash('自分は削除不可', 'warning')
            else: db_manager.delete_user(uid); flash('削除しました', 'info')
        return redirect(url_for('user_master'))
    users = db_manager.get_users()
    return render_template('user_master.html', users=users)

if __name__ == '__main__':
    app.run(debug=True)

"""
Pirates Trainer App - Main Application
Backend Logic (Flask)
Fixed: Karte Reuse (Copy) Functionality
"""
import os
import json
from datetime import datetime
from typing import Optional, Dict, Any, List, Union

from flask import Flask, render_template, request, redirect, url_for, abort, flash, session
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import check_password_hash
from dotenv import load_dotenv

from database import DatabaseManager

load_dotenv()

app = Flask(__name__)
app.config['SECRET_KEY'] = os.environ.get('SECRET_KEY', 'default_secret_key_for_local_test')
COACH_SHARED_PASSWORD = os.environ.get('COACH_PASSWORD', 'pirates')

login_manager = LoginManager()
login_manager.init_app(app)
login_manager.login_view = 'login'

db_manager = DatabaseManager()

class Const:
    STATUS_IN = 'IN (参加)'
    STATUS_RESTRICTION = 'RESTRICTION (制限付)'
    STATUS_OUT = 'OUT (不参加)'
    STATUS_GTD = 'GTD (当日判断)'
    
    TL_NONE = 'NON TIME LOSS'
    TL_NEW = 'NEW/RE-INJURY'
    TL_LOSS = 'TIME LOSS'
    TL_RTP = 'RETURN TO PLAY'

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

class User(UserMixin):
    def __init__(self, user_id, username, is_admin):
        self.id = user_id
        self.username = username
        self.is_admin = is_admin

@login_manager.user_loader
def load_user(user_id):
    user_data = db_manager._execute("SELECT user_id, username, is_admin FROM USER_MASTER WHERE user_id = %s", (user_id,))
    if user_data: 
        return User(user_data['user_id'], user_data['username'], user_data['is_admin'])
    return None

def prepare_karte_data(form_data: Dict[str, Any]) -> Dict[str, Any]:
    """フォーム入力をDB保存用形式に変換"""
    data = {
        'date': form_data.get('date'),
        'player_id': form_data.get('player_id') if form_data.get('player_id') else None,
        'tr': form_data.get('tr', ''),
        'time_loss_category': form_data.get('time_loss_category'),
        'diagnosis_flag': 1 if form_data.get('diagnosis_flag') == 'on' else 0,
        's_content': form_data.get('s_content'),
        'o_content': form_data.get('o_content'),
        'a_content': form_data.get('a_content'),
        'p_content': form_data.get('p_content'),
        'report_flag': 1 if form_data.get('report_flag') == 'on' else 0,
        'injury_name': form_data.get('injury_name', ''),
        'participation_status': form_data.get('participation_status', ''),
        'return_est': form_data.get('return_est', ''),
        'progress_note': form_data.get('progress_note', '')
    }
    for key in PULLDOWN_OPTIONS.keys():
        data[key] = form_data.get(key, '')
    return data

# --- ルート設定 ---

@app.route('/sys_update_db')
@login_required
def sys_update_db():
    if not current_user.is_admin: abort(403)
    if db_manager.migrate_schema():
        flash('データベースの更新が完了しました。', 'success')
        return redirect(url_for('index'))
    return "更新失敗", 500

@app.route('/login', methods=['GET', 'POST'])
def login():
    if current_user.is_authenticated: return redirect(url_for('index'))
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        user_data = db_manager._execute("SELECT * FROM USER_MASTER WHERE username = %s", (username,))
        if user_data and check_password_hash(user_data['password_hash'], password):
            login_user(User(user_data['user_id'], user_data['username'], user_data['is_admin']))
            return redirect(url_for('index'))
        flash('ログイン失敗', 'danger')
    return render_template('login.html')

@app.route('/logout')
def logout():
    logout_user()
    session.pop('coach_authenticated', None)
    return redirect(url_for('login'))

@app.route('/coach_login', methods=['GET', 'POST'])
def coach_login():
    if session.get('coach_authenticated'): return redirect(url_for('coach_view'))
    if request.method == 'POST':
        if request.form.get('password') == COACH_SHARED_PASSWORD:
            session['coach_authenticated'] = True
            return redirect(url_for('coach_view'))
        flash('合言葉が違います', 'danger')
    return render_template('coach_login.html')

@app.route('/coach_view')
def coach_view():
    if not current_user.is_authenticated and not session.get('coach_authenticated'):
        return redirect(url_for('coach_login'))
    
    reports = db_manager.get_coach_reports()
    priority_map = {Const.STATUS_OUT: 1, Const.STATUS_GTD: 2, Const.STATUS_RESTRICTION: 3, Const.STATUS_IN: 4}
    
    if reports:
        for row in reports:
            p_id = row.get('player_id')
            c_date = row.get('date')
            injury_date_str = db_manager.get_latest_injury_date(p_id, c_date)
            if injury_date_str:
                inj_dt = datetime.strptime(injury_date_str, '%Y-%m-%d')
                cur_dt = datetime.strptime(c_date, '%Y-%m-%d')
                diff = (cur_dt - inj_dt).days
                row['elapsed_days'] = f"Day {diff} (W{diff//7 + 1}D{diff%7})"
            else:
                row['elapsed_days'] = "-"
        reports.sort(key=lambda x: priority_map.get(x.get('participation_status'), 99))
    
    return render_template('coach_view.html', reports=reports, today=datetime.now().strftime('%Y-%m-%d'))

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
    data = db_manager.search_karty(filters)
    return render_template('index.html', data=data, player_list=db_manager.get_players(), 
                           filters=filters, TIME_LOSS_OPTIONS=TIME_LOSS_OPTIONS)

@app.route('/create_karte', methods=['GET', 'POST'])
@login_required
def create_karte():
    if request.method == 'POST':
        data = prepare_karte_data(request.form)
        if not data.get('player_id'):
            flash('エラー: 選手を選択してください。', 'danger')
            return render_template('karte_form.html', player_list=db_manager.get_players(),
                                   PULLDOWN_OPTIONS=PULLDOWN_OPTIONS, TIME_LOSS_OPTIONS=TIME_LOSS_OPTIONS,
                                   PARTICIPATION_STATUS_OPTIONS=PARTICIPATION_STATUS_OPTIONS,
                                   karte=data, action='create', today=datetime.now().strftime('%Y-%m-%d'))
        
        db_manager.create_karte(data)
        flash('カルテを作成しました', 'success')
        return redirect(url_for('index'))
    
    # 再利用作成（コピー）の処理
    copied_karte = None
    copy_from_id = request.args.get('copy_from_id')
    copy_player_id = request.args.get('copy_player_id')

    if copy_from_id:
        copied_karte = db_manager.get_karte(copy_from_id)
    elif copy_player_id:
        copied_karte = db_manager.get_latest_karte_by_player(copy_player_id)
    
    if copied_karte:
        copied_karte['date'] = datetime.now().strftime('%Y-%m-%d')
        copied_karte['karte_id'] = None # IDを消去して新規扱いにする

    return render_template('karte_form.html', player_list=db_manager.get_players(),
                           PULLDOWN_OPTIONS=PULLDOWN_OPTIONS, TIME_LOSS_OPTIONS=TIME_LOSS_OPTIONS,
                           PARTICIPATION_STATUS_OPTIONS=PARTICIPATION_STATUS_OPTIONS,
                           karte=copied_karte, action='create', today=datetime.now().strftime('%Y-%m-%d'))

@app.route('/karte/<int:karte_id>', methods=['GET', 'POST'])
@login_required
def edit_karte(karte_id):
    karte = db_manager.get_karte(karte_id)
    if not karte: abort(404)
    if request.method == 'POST':
        db_manager.update_karte(karte_id, prepare_karte_data(request.form))
        flash('カルテを更新しました', 'success')
        return redirect(url_for('edit_karte', karte_id=karte_id))
    
    return render_template('karte_form.html', karte=karte, player_list=db_manager.get_players(),
                           PULLDOWN_OPTIONS=PULLDOWN_OPTIONS, TIME_LOSS_OPTIONS=TIME_LOSS_OPTIONS,
                           PARTICIPATION_STATUS_OPTIONS=PARTICIPATION_STATUS_OPTIONS, action='edit')

@app.route('/karte/delete/<int:karte_id>', methods=['POST'])
@login_required
def delete_karte(karte_id):
    db_manager.delete_karte(karte_id)
    flash('カルテを削除しました', 'info')
    return redirect(url_for('index'))

# 他のマスタ管理、レポート、サマリーなどのルート（変更なし）
@app.route('/report')
@login_required
def report():
    report_data = db_manager.get_injury_report_data()
    site_summary = {}
    for item in report_data:
        site = item.get('injury_site', '不明')
        site_summary[site] = site_summary.get(site, 0) + item.get('count', 0)
    sorted_sites = sorted(site_summary.items(), key=lambda x: x[1], reverse=True)
    grouped_data = {}
    for item in report_data:
        cat = item.get('time_loss_category', 'OTHER')
        if cat not in grouped_data: grouped_data[cat] = []
        grouped_data[cat].append(item)
    return render_template('report.html', tl_counts=db_manager.get_all_time_loss_categories(), grouped_data=grouped_data,
                           chart_labels=json.dumps([x[0] for x in sorted_sites]),
                           chart_values=json.dumps([x[1] for x in sorted_sites]))

@app.route('/players', methods=['GET', 'POST'])
@login_required
def player_master():
    if request.method == 'POST':
        name = request.form.get('player_name', '').strip()
        if name and db_manager.add_player(name): flash(f'選手 {name} を登録しました', 'success')
        return redirect(url_for('player_master'))
    return render_template('player_master.html', players=db_manager.get_players())

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
    if not current_user.is_admin: abort(403)
    if request.method == 'POST':
        action = request.form.get('action')
        if action == 'add': db_manager.add_user(request.form.get('username'), request.form.get('password'), 1 if request.form.get('is_admin') else 0)
        elif action == 'delete': db_manager.delete_user(request.form.get('user_id'))
        return redirect(url_for('user_master'))
    return render_template('user_master.html', users=db_manager.get_users())

@app.route('/players/summary/<int:player_id>')
@login_required
def player_summary(player_id):
    player = db_manager.get_player(player_id)
    if not player: abort(404)
    return render_template('player_summary.html', player=player, summary=db_manager.get_player_summary_data(player_id))

if __name__ == '__main__':
    app.run(debug=True)

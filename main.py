import pandas as pd
import time
from urllib.parse import quote
from collections import defaultdict
from tqdm import tqdm

daily_df = pd.read_csv("./pitcher_10_2023_daily.csv", encoding='CP949')
players = [("페디", "1993-02-25", "NC"), ("안우진", "1999-08-30", "키움"), ("뷰캐넌", "1989-05-11", "삼성"), ("알칸타라", "1992-12-04", "두산"), ("임찬규", "1992-11-20", "LG"), ("문동주", "2003-12-23", "한화"), ("이의리", "2002-06-16", "KIA"), ("고영표", "1991-09-16", "KT"), ("김광현", "1988-07-22", "SSG"), ("반즈", "1995-10-01", "롯데")]
stadium_db = {"롯데": "부산사직야구장", "NC": "창원NC파크", "키움": "고척스카이돔", "삼성": "대구삼성라이온즈파크", "두산": "잠실야구장", "LG": "잠실야구장", "한화": "한화생명이글스파크", "KIA": "광주기아챔피언스필드", "KT": "수원KT위즈파크", "SSG": "인천SSG랜더스필드"}

db = defaultdict(list)

for player, birth, team in tqdm(players):
    print(f"###### {player} 시작 ######")
    # 선발인 경우만 저장
    for idx, daily in daily_df[daily_df['선수명'] == player].iterrows():
        if daily['구분'] != '선발':
            continue

        str_daily = str(daily['날짜']).split('.')
        pdate = "2023-"+str_daily[0]+"-"+str_daily[1]
        if len(str_daily[1]) == 1:
            pdate += "0"
        url = "http://www.statiz.co.kr/player.php?opt=6&name="+quote(player)+"&birth="+birth+"&re=1&da=1&year=2023&plist=&pdate="+pdate
        tables = pd.read_html(url)

        start, end = False, False
        score_1 = []
        top_or_bottom = "초"
        inning = 1
        home_team, away_team = "", ""
        score = 0
        for idx, row in tables[1].iterrows():
            # 초 공격인지 / 말 공격인지 체크 & 이닝 체크
            if "초" in row['이닝']:
                top_or_bottom = "초"
                inning = int(row['이닝'][0])
            else:
                top_or_bottom = "말"
                inning = int(row['이닝'][0])

                # 이닝 시작과 끝을 체크하여 해당 이닝에 기록된 실점 반환
            if start == False and '무사' in row['이전상황']:
                start = True
                score_1 = row['이전상황'].split(' ')[1].split(":")

            # 한이닝을 온전히 던진 경우
            if end == False and '이닝종료' in row['이후상황']:
                start = False
                end = True
                score_2 = row['이후상황'].split(' ')[1].split(":")
                if top_or_bottom == "초":
                    score = int(score_2[0]) - int(score_1[0])
                    home_team, away_team = team, row['상대']
                else:
                    score = int(score_2[1]) - int(score_1[1])
                    home_team, away_team = row['상대'], team

                    # 강판 당한 경우
            elif idx == len(tables[1])-1:
                end = True
                score_2 = row['이후상황'].split(' ')[-1].split(":")
                if top_or_bottom == "초":
                    score = int(score_2[0]) - int(score_1[0])
                    home_team, away_team = team, row['상대']
                else:
                    score = int(score_2[1]) - int(score_1[1])
                    home_team, away_team = row['상대'], team

                # 승계주자 있는 경우만 처리
                if len(row['이후상황'].split(' ')):
                    runners = 0
                    # 승계주자 몇명인지 체크
                    if row['이후상황'].split(' ')[1] == '만루':
                        runners = 3
                    else:
                        runners = len(row['이후상황'].split(','))

                    # 승계주자 실점했는지 체크
                    url_2 = "http://www.statiz.co.kr/boxscore.php?opt=1&sopt=0&date="+pdate
                    match_data = pd.read_html(url_2)[1:]
                    tables_2 = pd.DataFrame()
                    for idx, data in enumerate(match_data):
                        if home_team in list(match_data[idx]['팀']):
                            tables_2 = data
                            break

                    if top_or_bottom == '초':
                        score_3 = int(tables_2.iloc[0][str(inning)].split(' ')[0])
                        score += min(score_3-score, runners)
                    else:
                        score_3 = int(tables_2.iloc[1][str(inning)].split(' ')[0])
                        score += min(score_3-score, runners)

                        # db에 저장
            if end == True:
                db['날짜'].append(row['날짜'])
                db['상대'].append(row['상대'])
                db['이닝'].append(inning)
                db['투수'].append(player)
                db['실점'].append(score)
                db['Home Team'].append(home_team)
                db['Away Team'].append(away_team)
                db['구장'].append(stadium_db[home_team])
                end = False
    time.sleep(10)

    df = pd.DataFrame.from_dict(db)
# df.head()
#
# df.to_csv("pitcher_10_2023_daily_detail_2023_04_08.csv",index=False, na_rep='NaN')

df = pd.DataFrame.from_dict(db)
df.to_csv(f"pitcher_10_2023_daily_detail_{player}_{'_'.join(pdate.split('-'))}.csv", index=False, na_rep='NaN')
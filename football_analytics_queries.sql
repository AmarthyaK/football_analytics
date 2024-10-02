CREATE DATABASE football_analytics;

USE football_analytics;

#to know which team won with huge goal difference
SELECT COALESCE(H.name,'Not_in_records_team') AS Home_Team,COALESCE(A.name,'Not_in_records_team') AS Away_Team,C.name AS competition, score_fullTime_home AS Home_team_goals, score_fullTime_away AS Away_team_goals,
CASE
	WHEN m.score_winner = 'AWAY_TEAM' THEN m.score_fullTime_away-m.score_fullTime_home
    WHEN m.score_winner = 'HOME_TEAM' THEN m.score_fullTime_home - m.score_fullTime_away
    ELSE 0
END AS goal_diff
FROM matches m
LEFT JOIN teams H
ON m.homeTeam_id = H.id
LEFT JOIN teams A
ON m.awayTeam_id = A.id
LEFT JOIN competition C
ON m.competition_id = C.id
ORDER BY goal_diff DESC;

## To determine which league is more competitive by calcuating the average goal diff
SELECT  C.name AS competition,
AVG(CASE
	WHEN m.score_winner = 'AWAY_TEAM' THEN m.score_fullTime_away-m.score_fullTime_home
    WHEN m.score_winner = 'HOME_TEAM' THEN m.score_fullTime_home - m.score_fullTime_away
    ELSE 0
END) AS goal_diff_avg
FROM matches m
LEFT JOIN competition C
ON m.competition_id = C.id
GROUP BY C.name
ORDER BY goal_diff_avg ASC;

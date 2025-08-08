import json
import os

import pandas as pd
from sqlalchemy import (JSON, Column, Date, Float, ForeignKey, Integer, String,
                        create_engine)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

# --- Configuration ---
DB_FILE = "ipl.db"
DATA_DIR = "data"
DB_URL = f"sqlite:///{DB_FILE}"

# --- SQLAlchemy Setup ---
Base = declarative_base()
engine = create_engine(DB_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# --- SQLAlchemy ORM Models (Schema) ---

class Team(Base):
    __tablename__ = 'teams'
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)

class Player(Base):
    __tablename__ = 'players'
    id = Column(String, primary_key=True, index=True)
    name = Column(String, index=True)

class Match(Base):
    __tablename__ = 'matches'
    id = Column(String, primary_key=True, index=True)
    season = Column(String)
    venue = Column(String, nullable=True)
    city = Column(String, nullable=True)
    match_date = Column(Date)
    match_type = Column(String)
    balls_per_over = Column(Integer)
    team1_id = Column(Integer, ForeignKey('teams.id'))
    team2_id = Column(Integer, ForeignKey('teams.id'))
    toss_winner_id = Column(Integer, ForeignKey('teams.id'))
    toss_decision = Column(String)
    winner_id = Column(Integer, ForeignKey('teams.id'), nullable=True)
    outcome_result = Column(String, nullable=True)
    outcome_margin = Column(Integer, nullable=True)
    player_of_match_id = Column(String, ForeignKey('players.id'), nullable=True)
    officials = Column(JSON, nullable=True)
    innings = relationship("Inning", back_populates="match", cascade="all, delete-orphan")

class Inning(Base):
    __tablename__ = 'innings'
    id = Column(Integer, primary_key=True)
    match_id = Column(String, ForeignKey('matches.id'), index=True)
    inning_number = Column(Integer)
    batting_team_id = Column(Integer, ForeignKey('teams.id'))
    match = relationship("Match", back_populates="innings")
    deliveries = relationship("Delivery", back_populates="inning", cascade="all, delete-orphan")

class Delivery(Base):
    __tablename__ = 'deliveries'
    id = Column(Integer, primary_key=True)
    inning_id = Column(Integer, ForeignKey('innings.id'), index=True)
    over = Column(Float)
    batter_id = Column(String, ForeignKey('players.id'))
    bowler_id = Column(String, ForeignKey('players.id'))
    non_striker_id = Column(String, ForeignKey('players.id'))
    runs_batter = Column(Integer)
    runs_extras = Column(Integer)
    runs_total = Column(Integer)
    extras_type = Column(String, nullable=True)
    is_wicket = Column(Integer, default=0)
    player_out_id = Column(String, ForeignKey('players.id'), nullable=True)
    wicket_kind = Column(String, nullable=True)
    wicket_fielders = Column(JSON, nullable=True)
    inning = relationship("Inning", back_populates="deliveries")
    batter = relationship("Player", foreign_keys=[batter_id])
    bowler = relationship("Player", foreign_keys=[bowler_id])
    player_out = relationship("Player", foreign_keys=[player_out_id])


def create_database():
    if os.path.exists(DB_FILE):
        os.remove(DB_FILE)
        print(f"Removed existing database '{DB_FILE}'.")
    Base.metadata.create_all(bind=engine)
    print("Database and tables created successfully using SQLAlchemy.")


def process_and_load_data():
    session = SessionLocal()
    
    json_files = [f for f in os.listdir(DATA_DIR) if f.endswith('.json')]
    
    player_cache = {}
    team_cache = {}

    for file_name in json_files:
        print(f"Processing {file_name}...")
        with open(os.path.join(DATA_DIR, file_name), 'r') as f:
            data = json.load(f)
            
            info = data['info']
            
            # --- Get or Create Players and Teams in memory first ---
            if 'registry' in info and 'people' in info['registry']:
                for name, pid in info['registry']['people'].items():
                    if pid not in player_cache:
                        player_cache[pid] = Player(id=pid, name=name)
            
            for team_name in info['teams']:
                if team_name not in team_cache:
                    team_cache[team_name] = Team(name=team_name)

            # --- Add all new players and teams to session ---
            session.add_all(player_cache.values())
            session.add_all(team_cache.values())
            # Commit to ensure players/teams exist before we use their IDs
            session.commit()

            # --- Create Match object ---
            outcome = info.get('outcome', {})
            player_of_match_name = info.get('player_of_match', [None])[0]
            match_id = os.path.splitext(file_name)[0]
            
            # Find the player and winner team IDs from the now-committed data
            player_id = next((p.id for p in player_cache.values() if p.name == player_of_match_name), None)
            winner_team_id = next((t.id for t in team_cache.values() if t.name == outcome.get('winner')), None)

            new_match = Match(
                id=match_id,
                season=info.get('season'), venue=info.get('venue'), city=info.get('city'),
                match_date=pd.to_datetime(info['dates'][0]).date(),
                match_type=info.get('match_type'), balls_per_over=info.get('balls_per_over'),
                team1_id=next(t.id for t in team_cache.values() if t.name == info['teams'][0]),
                team2_id=next(t.id for t in team_cache.values() if t.name == info['teams'][1]),
                toss_winner_id=next(t.id for t in team_cache.values() if t.name == info['toss']['winner']),
                toss_decision=info['toss']['decision'],
                winner_id=winner_team_id,
                outcome_result=list(outcome.get('by', {}).keys())[0] if outcome.get('by') else None,
                outcome_margin=list(outcome.get('by', {}).values())[0] if outcome.get('by') else None,
                player_of_match_id=player_id,
                officials=info.get('officials')
            )

            # --- Create Inning and Delivery objects ---
            for i, inning_data in enumerate(data['innings']):
                batting_team_id = next(t.id for t in team_cache.values() if t.name == inning_data['team'])
                new_inning = Inning(inning_number=i + 1, batting_team_id=batting_team_id)
                
                for over_data in inning_data['overs']:
                    for delivery_data in over_data['deliveries']:
                        wicket_info = delivery_data.get('wickets', [{}])[0]
                        batter_id = next((p.id for p in player_cache.values() if p.name == delivery_data['batter']), None)
                        bowler_id = next((p.id for p in player_cache.values() if p.name == delivery_data['bowler']), None)
                        non_striker_id = next((p.id for p in player_cache.values() if p.name == delivery_data['non_striker']), None)
                        player_out_id = next((p.id for p in player_cache.values() if p.name == wicket_info.get('player_out')), None)
                        
                        new_delivery = Delivery(
                            over=over_data['over'] + (over_data['deliveries'].index(delivery_data) + 1) / 10.0,
                            batter_id=batter_id, bowler_id=bowler_id, non_striker_id=non_striker_id,
                            runs_batter=delivery_data['runs']['batter'],
                            runs_extras=delivery_data['runs']['extras'],
                            runs_total=delivery_data['runs']['total'],
                            extras_type=list(delivery_data.get('extras', {}).keys())[0] if 'extras' in delivery_data else None,
                            is_wicket=1 if wicket_info else 0,
                            player_out_id=player_out_id,
                            wicket_kind=wicket_info.get('kind'),
                            wicket_fielders=[f['name'] for f in wicket_info.get('fielders', [])] if wicket_info.get('fielders') else None,
                        )
                        new_inning.deliveries.append(new_delivery)
                new_match.innings.append(new_inning)
            
            session.add(new_match)
    
    try:
        session.commit()
        print("Successfully committed all data to the database.")
    except Exception as e:
        print(f"An error occurred during final commit: {e}")
        session.rollback()
    finally:
        session.close()

if __name__ == "__main__":
    create_database()
    process_and_load_data()
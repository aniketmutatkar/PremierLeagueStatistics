"""
FBRef Scraper - NEW FIXTURE-BASED IMPLEMENTATION
Phase 1: Scraper changes for team-specific gameweek tracking
"""
import requests
from bs4 import BeautifulSoup, Comment
import pandas as pd
import logging
import random
import time
import yaml
import re
from io import StringIO
from pathlib import Path
from typing import Dict, List, Optional, Union, Any, Tuple
from datetime import datetime

logger = logging.getLogger(__name__)

class FBRefScraper:
    """NEW: Fixture-based scraper with team-specific gameweek tracking"""
    
    def __init__(self, config_path: str = "config", override_season: str = None):
        self.config_path = Path(config_path)
        self.scraping_config = self._load_config("scraping.yaml")
        self.sources_config = self._load_config("sources.yaml")
        self.override_season = override_season
        
        logger.info("FBRef scraper initialized (NEW fixture-based mode)")
    
    def _load_config(self, filename: str) -> dict:
        """Load configuration from YAML file"""
        config_file = self.config_path / filename
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)
    
    # ==================== NEW FIXTURE-BASED LOGIC ====================
    
    def _determine_team_gameweeks(self, fixtures_df: pd.DataFrame) -> Dict[str, int]:
        """
        NEW: Calculate the latest completed gameweek for each team
        
        Args:
            fixtures_df: DataFrame with fixture data
            
        Returns:
            Dict mapping team_name -> latest_completed_gameweek
            e.g., {"Manchester City": 6, "Liverpool": 5}
        """
        if 'gameweek' not in fixtures_df.columns:
            logger.warning("No gameweek column in fixtures")
            return {}
        
        team_gameweeks = {}
        
        # Get all unique teams from both home and away
        home_teams = set(fixtures_df['home_team'].dropna().unique())
        away_teams = set(fixtures_df['away_team'].dropna().unique())
        all_teams = home_teams | away_teams
        
        logger.debug(f"Calculating gameweeks for {len(all_teams)} teams")
        
        for team in all_teams:
            # Find all completed fixtures for this team
            team_fixtures = fixtures_df[
                ((fixtures_df['home_team'] == team) | (fixtures_df['away_team'] == team)) &
                (fixtures_df['is_completed'] == True)
            ]
            
            if not team_fixtures.empty:
                # Latest completed gameweek for this team
                latest_gw = int(team_fixtures['gameweek'].max())
                team_gameweeks[team] = latest_gw
                logger.debug(f"  {team}: latest completed GW = {latest_gw}")
            else:
                # Team hasn't played any matches yet
                team_gameweeks[team] = 0
                logger.debug(f"  {team}: no completed matches")
        
        return team_gameweeks
    
    def get_gameweek_status(self, fixtures_df: pd.DataFrame) -> Dict[str, Any]:
        """
        NEW: Get comprehensive gameweek status across all teams
        
        Args:
            fixtures_df: DataFrame with fixture data
            
        Returns:
            Dict with detailed gameweek info:
            {
                'team_gameweeks': Dict[str, int],
                'min_gameweek': int,
                'max_gameweek': int,
                'teams_behind': List[str],
                'all_teams_aligned': bool
            }
        """
        team_gameweeks = self._determine_team_gameweeks(fixtures_df)
        
        if not team_gameweeks:
            return {
                'team_gameweeks': {},
                'min_gameweek': 0,
                'max_gameweek': 0,
                'teams_behind': [],
                'all_teams_aligned': True
            }
        
        gameweek_values = list(team_gameweeks.values())
        min_gw = min(gameweek_values)
        max_gw = max(gameweek_values)
        
        # Find teams that are behind the maximum
        teams_behind = [team for team, gw in team_gameweeks.items() if gw < max_gw]
        
        status = {
            'team_gameweeks': team_gameweeks,
            'min_gameweek': min_gw,
            'max_gameweek': max_gw,
            'teams_behind': teams_behind,
            'all_teams_aligned': (min_gw == max_gw)
        }
        
        logger.info(f"Gameweek status: min={min_gw}, max={max_gw}, teams_behind={len(teams_behind)}")
        if teams_behind:
            logger.info(f"  Teams behind: {teams_behind}")
        
        return status
    
    def scrape_fixtures(self, source: Union[str, Path]) -> Dict[str, Any]:
        """
        NEW: Scrape fixture data with team-specific gameweek tracking
        
        Returns:
            {
                'fixtures': DataFrame with all fixtures,
                'gameweek_status': Dict with team-specific gameweeks,
                'season': str
            }
        """
        logger.info(f"Scraping fixtures from {source}")
        
        all_tables = self._extract_tables_from_html(source)
        
        if not all_tables:
            logger.error("No tables extracted from fixtures")
            return {}
        
        fixture_table = self._identify_fixture_table(all_tables)
        
        if fixture_table is None:
            logger.error("Could not identify fixtures table")
            return {}
        
        fixtures_df = self._process_fixture_table(fixture_table)
        
        # NEW: Get team-specific gameweek status
        gameweek_status = self.get_gameweek_status(fixtures_df)
        
        season = self._extract_season_info()
        
        result = {
            'fixtures': fixtures_df,
            'gameweek_status': gameweek_status,  # NEW: Dict instead of single int
            'season': season
        }
        
        logger.info(f"✅ Extracted {len(fixtures_df)} fixtures")
        logger.info(f"   Gameweek range: {gameweek_status['min_gameweek']}-{gameweek_status['max_gameweek']}")
        logger.info(f"   Teams aligned: {gameweek_status['all_teams_aligned']}")
        
        return result
    
    # ==================== UNCHANGED METHODS ====================
    
    def scrape_single_stat_category(self, source: Union[str, Path], stat_type: str) -> Dict[str, pd.DataFrame]:
        """
        Scrape a single stat category and return 3 clean tables (UNCHANGED)
        """
        logger.info(f"Scraping {stat_type} stats from {source}")
        
        all_tables = self._extract_tables_from_html(source)
        
        if not all_tables or len(all_tables) < 3:
            logger.error(f"Insufficient tables extracted from {source}")
            return {}
        
        try:
            squad_table, opponent_table, player_table = self._identify_stat_tables(all_tables)
            
            if squad_table is None or opponent_table is None or player_table is None:
                logger.error(f"Could not identify all 3 stat tables in {source}")
                return {}
            
            cleaned_tables = self._clean_stat_data_archive_method([squad_table, opponent_table, player_table])
            
            return {
                f'squad_{stat_type}': cleaned_tables[0],
                f'opponent_{stat_type}': cleaned_tables[1],
                f'player_{stat_type}': cleaned_tables[2]
            }
            
        except Exception as e:
            logger.error(f"Failed to process {stat_type} stats: {e}")
            return {}
    
    def scrape_all_stat_categories(self, sources: Dict[str, str]) -> Dict[str, pd.DataFrame]:
        """Scrape multiple stat categories individually (UNCHANGED)"""
        logger.info(f"Scraping {len(sources)} stat categories individually...")
        
        all_clean_tables = {}
        successful_categories = []
        failed_categories = []
        
        for stat_type, source in sources.items():
            try:
                if len(all_clean_tables) > 0:
                    delay = self.scraping_config['scraping']['delays']['between_requests']
                    logger.info(f"Rate limiting: waiting {delay}s...")
                    time.sleep(delay)
                
                category_tables = self.scrape_single_stat_category(source, stat_type)
                
                if category_tables:
                    all_clean_tables.update(category_tables)
                    successful_categories.append(stat_type)
                    logger.info(f"✅ Successfully scraped {stat_type}: {len(category_tables)} tables")
                else:
                    failed_categories.append(stat_type)
                    logger.error(f"❌ Failed to scrape {stat_type}")
                
            except Exception as e:
                logger.error(f"❌ Error scraping {stat_type}: {e}")
                failed_categories.append(stat_type)
        
        logger.info(f"Scraping complete: {len(successful_categories)} successful, {len(failed_categories)} failed")
        logger.info(f"✅ Successful: {successful_categories}")
        if failed_categories:
            logger.warning(f"❌ Failed: {failed_categories}")
        
        return all_clean_tables
    
    def _extract_season_info(self) -> str:
        """Extract season info (UNCHANGED - already correct)"""
        if self.override_season:
            return self.override_season
        
        current_year = datetime.now().year
        if datetime.now().month >= 8:
            return f"{current_year}-{current_year + 1}"
        else:
            return f"{current_year - 1}-{current_year}"
    
    def _clean_stat_data_archive_method(self, rawdata: List[pd.DataFrame]) -> List[pd.DataFrame]:
        """
        Complete archive cleaning method - main branch logic + level_0 fix
        """
        logger.debug("Cleaning stat data using archive method...")
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        if len(rawdata) < 3:
            raise ValueError("Expected at least three DataFrames")

        cleaned_tables = []
        
        for i, df in enumerate(rawdata):
            logger.debug(f"Cleaning DataFrame {i+1}/{len(rawdata)}")
            
            # Copy dataframe
            df = df.copy()
            
            # Join multi-level columns
            df.columns = [' '.join(col).strip() for col in df.columns]
            
            # Reset index (MAIN BRANCH HAS THIS)
            df = df.reset_index(drop=True)
            
            # Strip level_0 prefixes (FIX FOR NEW HTML STRUCTURE)
            new_columns = []
            for cols in df.columns:
                if 'level_0' in cols:
                    new_col = cols.split()[-1]  # takes the last name
                else:
                    new_col = cols
                new_columns.append(new_col)
            
            df.columns = new_columns
            
            # Fill NA with 0 (MAIN BRANCH HAS THIS)
            df = df.fillna(0)

            # Add current date column
            df['Current Date'] = current_date
            
            cleaned_tables.append(df)

        # Rename DataFrames
        SquadStats = cleaned_tables[0]
        OpponentStats = cleaned_tables[1]
        PlayerStats = cleaned_tables[2]

        # Format Player Columns (MAIN BRANCH HAS THIS)
        if 'Age' in PlayerStats.columns:
            PlayerStats['Age'] = PlayerStats['Age'].str[:2]
        if 'Nation' in PlayerStats.columns:
            PlayerStats['Nation'] = PlayerStats['Nation'].str.split(' ').str.get(1)
        if 'Rk' in PlayerStats.columns:
            PlayerStats = PlayerStats.drop(columns=['Rk'], errors='ignore')
        if 'Matches' in PlayerStats.columns:
            PlayerStats = PlayerStats.drop(columns=['Matches'], errors='ignore')

        # Drop rows with NaN (CRITICAL - MAIN BRANCH HAS THIS)
        initial_player_count = len(PlayerStats)
        PlayerStats.dropna(inplace=True)
        final_player_count = len(PlayerStats)
        logger.debug(f"Dropped {initial_player_count - final_player_count} rows with NaN values from PlayerStats.")

        # Convert numeric columns
        for col in SquadStats.columns[1:-1]:
            SquadStats[col] = pd.to_numeric(SquadStats[col], errors='coerce')
        for col in OpponentStats.columns[1:-1]:
            OpponentStats[col] = pd.to_numeric(OpponentStats[col], errors='coerce')
        for col in PlayerStats.columns[4:-1]:
            PlayerStats[col] = pd.to_numeric(PlayerStats[col], errors='coerce')

        logger.debug("Archive cleaning method completed successfully")
        
        return [SquadStats, OpponentStats, PlayerStats]

    def _identify_stat_tables(self, tables: List[pd.DataFrame]) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """Identify squad, opponent, and player tables (UNCHANGED)"""
        if len(tables) < 3:
            return None, None, None
        return tables[0], tables[1], tables[2]
    
    def _extract_tables_from_html(self, source: Union[str, Path]) -> List[pd.DataFrame]:
        """Extract tables from HTML (UNCHANGED)"""
        if isinstance(source, (str, Path)) and Path(source).exists():
            with open(source, 'r', encoding='utf-8') as file:
                html_content = file.read()
        elif isinstance(source, str) and source.startswith('http'):
            html_content = self._fetch_url(source)
        else:
            raise ValueError(f"Invalid source: {source}")
        
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = []
        
        try:
            html_tables = soup.find_all('table')
            for table in html_tables:
                df = pd.read_html(StringIO(str(table)))[0]
                tables.append(df)
            
            comments = soup.find_all(string=lambda text: isinstance(text, Comment))
            
            for comment in comments:
                comment_content = str(comment)
                
                if '<' in comment_content and '>' in comment_content:
                    comment_soup = BeautifulSoup(comment_content, 'html.parser')
                    table_container = comment_soup.find('div', class_='table_container')
                    
                    if table_container:
                        table_tag = table_container.find('table')
                        if table_tag:
                            try:
                                df = pd.read_html(StringIO(str(table_tag)))[0]
                                tables.append(df)
                            except Exception:
                                pass
            
            logger.debug(f"Extracted {len(tables)} tables from HTML")
            return tables
            
        except Exception as e:
            logger.error(f"Error extracting tables: {e}")
            return []
    
    def _fetch_url(self, url: str) -> str:
        """Fetch URL with rate limiting (UNCHANGED)"""
        max_attempts = self.scraping_config['scraping']['retries']['max_attempts']
        backoff_factor = self.scraping_config['scraping']['retries']['backoff_factor']
        
        for attempt in range(max_attempts):
            try:
                user_agents = self.scraping_config['scraping']['http']['user_agents']
                user_agent = random.choice(user_agents)
                
                headers = self.scraping_config['scraping']['http']['headers'].copy()
                headers['User-Agent'] = user_agent
                
                timeout = self.scraping_config['scraping']['http']['timeout']
                response = requests.get(url, headers=headers, timeout=timeout)
                response.raise_for_status()
                
                return response.text
                
            except requests.exceptions.RequestException as e:
                if attempt < max_attempts - 1:
                    error_delay = self.scraping_config['scraping']['delays']['on_error']
                    wait_time = error_delay * (backoff_factor ** attempt)
                    time.sleep(wait_time)
                else:
                    raise e
        
        raise RuntimeError(f"Failed to fetch {url}")
    
    def _identify_fixture_table(self, tables: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
        """Identify fixture table (UNCHANGED)"""
        for i, table in enumerate(tables):
            if len(table) < 10:
                continue
            columns = [str(col).lower() for col in table.columns]
            fixture_indicators = ['wk', 'date', 'time', 'home', 'away', 'score']
            matches = sum(1 for indicator in fixture_indicators if any(indicator in col for col in columns))
            if matches >= 4:
                return table
        return None
    
    def _process_fixture_table(self, table: pd.DataFrame) -> pd.DataFrame:
        """Process fixture table with season info for unique fixture IDs"""
        df = table.copy()
        df.columns = [' '.join(col).strip() if isinstance(col, tuple) else str(col).strip() for col in df.columns]
        
        column_mapping = {
            'Wk': 'gameweek', 'Day': 'day_of_week', 'Date': 'match_date',
            'Time': 'match_time', 'Home': 'home_team', 'Away': 'away_team',
            'Score': 'score', 'Attendance': 'attendance', 'Venue': 'venue',
            'Referee': 'referee'
        }

        df = df.rename(columns=column_mapping)

        # Add season info BEFORE creating fixture_id
        season = self._extract_season_info()
        df['season'] = season

        df = self._clean_fixture_data(df)
        return df
    
    def _clean_fixture_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean fixture data (UNCHANGED)"""
        df = df.copy()
        
        if 'gameweek' in df.columns:
            df = df[~df['gameweek'].astype(str).str.contains('Wk|Day|Date', case=False, na=False)]
            df['gameweek'] = df['gameweek'].ffill()
            df['gameweek'] = pd.to_numeric(df['gameweek'], errors='coerce')
            df = df.dropna(subset=['gameweek'])
        
        if 'match_date' in df.columns:
            df['match_date'] = pd.to_datetime(df['match_date'], errors='coerce')
        
        if 'match_time' in df.columns:
            df['match_time'] = df['match_time'].apply(self._parse_time)
        
        if 'score' in df.columns:
            df[['home_score', 'away_score']] = df['score'].apply(lambda x: pd.Series(self._parse_score(x)))
        
        df['is_completed'] = df['score'].notna() & (df['score'] != '') & (~df['score'].astype(str).str.contains('Head-to-Head|Notes', na=False))
        df['fixture_id'] = df.apply(self._create_fixture_id, axis=1)
        df['scraped_date'] = datetime.now().date()
        
        df = df.dropna(subset=['home_team', 'away_team'])
        df = df[~df['home_team'].astype(str).str.contains('Home|Team', case=False, na=False)]
        
        return df
    
    def _parse_time(self, time_str) -> Optional[str]:
        """Parse time string (UNCHANGED)"""
        if pd.isna(time_str) or time_str == '':
            return None
        
        time_str = str(time_str).strip()
        if '(' in time_str:
            time_str = time_str.split('(')[0].strip()
        
        time_match = re.match(r'(\d{1,2}:\d{2})', time_str)
        if time_match:
            return time_match.group(1) + ':00'
        
        return None
    
    def _parse_score(self, score_str) -> tuple[Optional[int], Optional[int]]:
        """Parse score string (UNCHANGED)"""
        if pd.isna(score_str) or score_str == '' or 'Head-to-Head' in str(score_str):
            return None, None
        
        try:
            score_str = str(score_str).replace('–', '-').replace('—', '-')
            if '-' in score_str:
                parts = score_str.split('-')
                if len(parts) == 2:
                    return int(parts[0].strip()), int(parts[1].strip())
        except:
            pass
        
        return None, None
    
    def _create_fixture_id(self, row) -> str:
        """
        Create unique fixture ID with season, gameweek, and full team names.

        Format: {season}_GW{gameweek}_{home_team}_vs_{away_team}
        Example: 2024-2025_GW7_ManchesterCity_vs_LeedsUnited

        FIXED: Removed 10-character truncation to prevent collisions between
        teams like Manchester City and Manchester United.
        """
        try:
            season = str(row.get('season', 'UNKNOWN'))
            gw = int(row['gameweek']) if pd.notna(row['gameweek']) else 0
            # Remove spaces but DON'T truncate - use full team names
            home = str(row['home_team']).replace(' ', '').replace("'", '')
            away = str(row['away_team']).replace(' ', '').replace("'", '')
            return f"{season}_GW{gw}_{home}_vs_{away}"
        except Exception as e:
            logger.warning(f"Failed to create fixture_id: {e}")
            return f"UNKNOWN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
"""
FBRef Scraper - Simplified for raw database (following archive pattern exactly)
Each stat category processed individually, no consolidation
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
    """Simplified scraper following archive pattern - individual stat categories only"""
    
    def __init__(self, config_path: str = "config", override_season: str = None):
        self.config_path = Path(config_path)
        self.scraping_config = self._load_config("scraping.yaml")
        self.sources_config = self._load_config("sources.yaml")
        self.override_season = override_season
        
        logger.info("FBRef scraper initialized (raw database mode)")
    
    def _load_config(self, filename: str) -> dict:
        """Load configuration from YAML file"""
        config_file = self.config_path / filename
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)
    
    def scrape_single_stat_category(self, source: Union[str, Path], stat_type: str) -> Dict[str, pd.DataFrame]:
        """
        Scrape a single stat category and return 3 clean tables (archive pattern)
        
        Args:
            source: File path or URL
            stat_type: Type of stats (standard, passing, shooting, etc.)
            
        Returns:
            Dict with 3 clean tables: {squad_standard: df, opponent_standard: df, player_standard: df}
        """
        logger.info(f"Scraping {stat_type} stats from {source}")
        
        # Extract all tables from HTML
        all_tables = self._extract_tables_from_html(source)
        
        if not all_tables or len(all_tables) < 3:
            logger.error(f"Insufficient tables extracted from {source}")
            return {}
        
        # Use your archive logic - get tables by position (this was working)
        try:
            # Based on your archive: squad=table[0], opponent=table[1], player=table[2] after extraction
            # But we need to find the right tables first
            squad_table, opponent_table, player_table = self._identify_stat_tables(all_tables)
            
            if squad_table is None or opponent_table is None or player_table is None:
                logger.error(f"Could not identify all 3 stat tables in {source}")
                return {}
            
            # Clean using your EXACT archive cleaning logic
            cleaned_tables = self._clean_stat_data_archive_method([squad_table, opponent_table, player_table])
            
            # Return with proper table names (archive pattern)
            return {
                f'squad_{stat_type}': cleaned_tables[0],
                f'opponent_{stat_type}': cleaned_tables[1],
                f'player_{stat_type}': cleaned_tables[2]
            }
            
        except Exception as e:
            logger.error(f"Failed to process {stat_type} stats: {e}")
            return {}
    
    def scrape_all_stat_categories(self, sources: Dict[str, str]) -> Dict[str, pd.DataFrame]:
        """
        Scrape multiple stat categories individually (no consolidation)
        
        Args:
            sources: Dict mapping stat_type -> source_path
            
        Returns:
            Dict with all clean tables: {squad_standard: df, player_passing: df, ...}
        """
        logger.info(f"Scraping {len(sources)} stat categories individually...")
        
        all_clean_tables = {}
        successful_categories = []
        failed_categories = []
        
        for stat_type, source in sources.items():
            try:
                # Apply rate limiting between requests
                if len(all_clean_tables) > 0:  # Not first request
                    delay = self.scraping_config['scraping']['delays']['between_requests']
                    logger.info(f"Rate limiting: waiting {delay}s...")
                    time.sleep(delay)
                
                # Process single category
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
    
    def scrape_fixtures(self, source: Union[str, Path]) -> Dict[str, Any]:
        """Scrape fixture data (unchanged from before)"""
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
        current_gameweek = self._determine_current_gameweek(fixtures_df)
        season = self._extract_season_info()
        
        result = {
            'fixtures': fixtures_df,
            'current_gameweek': current_gameweek,
            'season': season
        }
        
        logger.info(f"✅ Extracted {len(fixtures_df)} fixtures, current gameweek: {current_gameweek}")
        return result
    
    def _clean_stat_data_archive_method(self, rawdata: List[pd.DataFrame]) -> List[pd.DataFrame]:
        """
        Your EXACT archive cleaning method from datacleaner.py
        This is the proven working logic
        """
        logger.debug("Cleaning stat data using archive method...")
        current_date = datetime.now().strftime('%Y-%m-%d')
        
        if len(rawdata) < 3:
            raise ValueError("Expected at least three DataFrames")

        cleaned_tables = []
        
        for i, df in enumerate(rawdata):
            logger.debug(f"Cleaning DataFrame {i+1}/{len(rawdata)}")
            
            # Your exact archive logic
            df = df.copy()
            df.columns = [' '.join(col).strip() for col in df.columns]
            df = df.reset_index(drop=True)
            
            new_columns = []
            for cols in df.columns:
                if 'level_0' in cols:
                    new_col = cols.split()[-1]  # takes the last name
                else:
                    new_col = cols
                new_columns.append(new_col)
            
            df.columns = new_columns
            df = df.fillna(0)

            # Add a column with the current date
            df['Current Date'] = current_date
            
            cleaned_tables.append(df)

        # Rename DataFrames (your archive logic)
        SquadStats = cleaned_tables[0]
        OpponentStats = cleaned_tables[1]
        PlayerStats = cleaned_tables[2]

        # Format Columns in Player Standard Stats DataFrame (your archive logic)
        if 'Age' in PlayerStats.columns:
            PlayerStats['Age'] = PlayerStats['Age'].str[:2]
        if 'Nation' in PlayerStats.columns:
            PlayerStats['Nation'] = PlayerStats['Nation'].str.split(' ').str.get(1)
        if 'Rk' in PlayerStats.columns:
            PlayerStats = PlayerStats.drop(columns=['Rk'], errors='ignore')
        if 'Matches' in PlayerStats.columns:
            PlayerStats = PlayerStats.drop(columns=['Matches'], errors='ignore')

        # Drop all the rows that have NaN in the row (CRITICAL archive step)
        initial_player_count = len(PlayerStats)
        PlayerStats.dropna(inplace=True)
        final_player_count = len(PlayerStats)
        
        logger.debug(f"Dropped {initial_player_count - final_player_count} rows with NaN values from PlayerStats.")

        # Convert all the Data types of the numeric columns from object to numeric (your archive logic)
        for col in SquadStats.columns[1:-1]:
            SquadStats[col] = pd.to_numeric(SquadStats[col], errors='coerce')
        for col in OpponentStats.columns[1:-1]:
            OpponentStats[col] = pd.to_numeric(OpponentStats[col], errors='coerce')
        for col in PlayerStats.columns[4:-1]:
            PlayerStats[col] = pd.to_numeric(PlayerStats[col], errors='coerce')

        logger.debug("Archive cleaning method completed successfully")
        
        return [SquadStats, OpponentStats, PlayerStats]
    
    # All the helper methods remain the same as before
    def _extract_tables_from_html(self, source: Union[str, Path]) -> List[pd.DataFrame]:
        """Extract tables from HTML (unchanged)"""
        # Get HTML content
        if isinstance(source, (str, Path)) and Path(source).exists():
            with open(source, 'r', encoding='utf-8') as file:
                html_content = file.read()
        elif isinstance(source, str) and source.startswith('http'):
            html_content = self._fetch_url(source)
        else:
            raise ValueError(f"Invalid source: {source}")
        
        # Extract tables using proven logic
        soup = BeautifulSoup(html_content, 'html.parser')
        tables = []
        
        try:
            # Find all table tags
            html_tables = soup.find_all('table')
            for table in html_tables:
                df = pd.read_html(StringIO(str(table)))[0]
                tables.append(df)
            
            # Find tables in comments
            comments = soup.find_all(string=lambda text: isinstance(text, Comment))
            
            for comment in comments:
                comment_content = str(comment)
                
                if '<' in comment_content and '>' in comment_content:
                    comment_soup = BeautifulSoup(comment_content, 'html.parser')
                    table_container = comment_soup.find('div', class_='table_container')
                    
                    if table_container:
                        df = pd.read_html(StringIO(str(table_container)))[0]
                        tables.append(df)
            
            logger.debug(f"Extracted {len(tables)} total tables")
            return tables
            
        except Exception as e:
            logger.error(f"Error extracting tables: {e}")
            return []
    
    def _identify_stat_tables(self, tables: List[pd.DataFrame]) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[pd.DataFrame]]:
        """Identify squad, opponent, and player stat tables"""
        squad_table = None
        opponent_table = None
        player_table = None
        
        for i, table in enumerate(tables):
            if len(table) < 15:
                continue
                
            columns = [str(col).lower() for col in table.columns]
            
            # Player table: has 'player' column and 250+ rows
            if any('player' in col for col in columns):
                logger.debug(f"Identified player table: Table {i} with {len(table)} rows")
                player_table = table
                
            # Squad tables: have 'squad' column and ~20 rows
            elif any('squad' in col for col in columns):
                first_squad_col = None
                for col in table.columns:
                    if 'squad' in str(col).lower():
                        first_squad_col = col
                        break
                
                if first_squad_col is not None:
                    sample_squads = table[first_squad_col].dropna().astype(str).head(3).tolist()
                    if any('vs ' in squad for squad in sample_squads):
                        logger.debug(f"Identified opponent table: Table {i} with {len(table)} rows")
                        opponent_table = table
                    else:
                        logger.debug(f"Identified squad table: Table {i} with {len(table)} rows") 
                        squad_table = table
        
        return squad_table, opponent_table, player_table
    
    def _fetch_url(self, url: str) -> str:
        """Fetch URL with rate limiting (unchanged)"""
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
    
    # Fixture processing methods remain unchanged
    def _identify_fixture_table(self, tables: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
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
        df = table.copy()
        df.columns = [' '.join(col).strip() if isinstance(col, tuple) else str(col).strip() for col in df.columns]
        
        column_mapping = {
            'Wk': 'gameweek', 'Day': 'day_of_week', 'Date': 'match_date',
            'Time': 'match_time', 'Home': 'home_team', 'Away': 'away_team',
            'Score': 'score', 'Attendance': 'attendance', 'Venue': 'venue',
            'Referee': 'referee'
        }
        
        df = df.rename(columns=column_mapping)
        df = self._clean_fixture_data(df)
        return df
    
    def _clean_fixture_data(self, df: pd.DataFrame) -> pd.DataFrame:
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
        try:
            gw = int(row['gameweek']) if pd.notna(row['gameweek']) else 0
            home = str(row['home_team']).replace(' ', '')[:10]
            away = str(row['away_team']).replace(' ', '')[:10]
            return f"GW{gw}_{home}_vs_{away}"
        except:
            return f"UNKNOWN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def _determine_current_gameweek(self, fixtures_df: pd.DataFrame) -> int:
        if 'gameweek' not in fixtures_df.columns:
            return 1
        
        gameweeks = sorted([int(gw) for gw in fixtures_df['gameweek'].dropna().unique()])
        
        for gw in gameweeks:
            gw_fixtures = fixtures_df[fixtures_df['gameweek'] == gw]
            incomplete_matches = gw_fixtures['is_completed'] == False
            
            if incomplete_matches.any():
                return int(gw)
        
        return max(gameweeks) + 1 if gameweeks else 1
    
    def _extract_season_info(self) -> str:
        # If we have an override (for historical), use it
        if self.override_season:
            return self.override_season
        
        # Otherwise, use normal logic (for current data)
        current_year = datetime.now().year
        if datetime.now().month >= 8:
            return f"{current_year}-{current_year + 1}"
        else:
            return f"{current_year - 1}-{current_year}"
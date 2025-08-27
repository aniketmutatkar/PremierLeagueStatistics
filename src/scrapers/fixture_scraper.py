"""
Fixture Scraper - Extract Premier League fixtures and gameweek information
"""
import pandas as pd
import logging
from datetime import datetime, date
from typing import Dict, List, Tuple, Optional, Any
from .base_scraper import BaseScraper

logger = logging.getLogger(__name__)

class FixtureScraper(BaseScraper):
    """
    Scraper for Premier League fixtures and gameweek detection
    """
    
    def scrape_fixtures(self, source) -> Dict[str, Any]:
        """
        Scrape fixtures and return structured data
        
        Returns:
            Dict with:
            - 'fixtures': DataFrame with all fixtures
            - 'current_gameweek': Current gameweek number  
            - 'gameweeks': DataFrame with gameweek metadata
            - 'season': Season string (e.g., '2025-2026')
        """
        logger.info(f"Starting fixture scrape of {source}")
        
        # Get all tables using base scraper
        all_tables = self.scrape_tables(source)
        
        if not all_tables:
            logger.error("No tables extracted from fixtures")
            return {}
        
        # Find the main fixtures table (usually the largest one)
        fixture_table = self._identify_fixture_table(all_tables)
        
        if fixture_table is None:
            logger.error("Could not identify fixtures table")
            return {}
        
        # Process the fixture table
        fixtures_df = self._process_fixture_table(fixture_table)
        
        # Extract gameweek information
        gameweeks_df = self._extract_gameweeks(fixtures_df)
        
        # Determine current gameweek
        current_gameweek = self._determine_current_gameweek(fixtures_df)
        
        # Extract season info
        season = self._extract_season_info(fixture_table)
        
        result = {
            'fixtures': fixtures_df,
            'current_gameweek': current_gameweek,
            'gameweeks': gameweeks_df,
            'season': season
        }
        
        logger.info(f"✅ Extracted {len(fixtures_df)} fixtures across {len(gameweeks_df)} gameweeks")
        logger.info(f"   Current gameweek: {current_gameweek}")
        logger.info(f"   Season: {season}")
        
        return result
    
    def _identify_fixture_table(self, tables: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
        """
        Identify the main fixtures table
        Should have columns like Wk, Date, Time, Home, Away, Score
        """
        for i, table in enumerate(tables):
            if len(table) < 10:  # Skip small tables
                continue
                
            columns = [str(col).lower() for col in table.columns]
            
            # Look for fixture table indicators
            fixture_indicators = ['wk', 'date', 'time', 'home', 'away', 'score']
            matches = sum(1 for indicator in fixture_indicators if any(indicator in col for col in columns))
            
            if matches >= 4:  # Must match at least 4 indicators
                logger.info(f"Identified fixtures table: Table {i} with {len(table)} rows")
                return table
        
        return None
    
    def _process_fixture_table(self, table: pd.DataFrame) -> pd.DataFrame:
        """
        Clean and process the fixtures table
        """
        df = table.copy()
        
        # Clean column names (remove multi-level if present)
        df.columns = [' '.join(col).strip() if isinstance(col, tuple) else str(col).strip() for col in df.columns]
        
        # Standardize column names
        column_mapping = {
            'Wk': 'gameweek',
            'Day': 'day_of_week', 
            'Date': 'match_date',
            'Time': 'match_time',
            'Home': 'home_team',
            'Away': 'away_team',
            'Score': 'score',
            'xG': 'home_xg',  # First xG column
            'Attendance': 'attendance',
            'Venue': 'venue',
            'Referee': 'referee',
            'Match Report': 'match_report',
            'Notes': 'notes'
        }
        
        # Apply column mapping
        df = df.rename(columns=column_mapping)
        
        # Handle the two xG columns (home_xg and away_xg)
        xg_columns = [col for col in df.columns if 'xg' in str(col).lower()]
        if len(xg_columns) >= 2:
            df = df.rename(columns={xg_columns[1]: 'away_xg'})
        
        # Clean and process data
        df = self._clean_fixture_data(df)
        
        return df
    
    def _parse_time(self, time_str) -> Optional[str]:
        """
        Parse time strings like "20:00 (15:00)" to simple "20:00" format for database
        """
        if pd.isna(time_str) or time_str == '':
            return None
        
        time_str = str(time_str).strip()
        
        # Extract the first time from formats like "20:00 (15:00)"
        if '(' in time_str:
            time_str = time_str.split('(')[0].strip()
        
        # Extract time from formats that might have extra text
        import re
        time_match = re.match(r'(\d{1,2}:\d{2})', time_str)
        if time_match:
            return time_match.group(1) + ':00'  # Add seconds for database compatibility
        
        return None
    
    def _clean_fixture_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean fixture data types and handle missing values
        """
        # Make explicit copy to avoid pandas warnings
        df = df.copy()
        
        # Remove header rows that got mixed into data
        if 'gameweek' in df.columns:
            # Remove rows where gameweek contains 'Wk' or other non-numeric values
            df = df[~df['gameweek'].astype(str).str.contains('Wk|Day|Date', case=False, na=False)]
            
            # Forward fill gameweek numbers (they're only shown on first match of each gameweek)
            df['gameweek'] = df['gameweek'].ffill()
            
            # Convert to numeric, dropping any remaining non-numeric values
            df['gameweek'] = pd.to_numeric(df['gameweek'], errors='coerce')
            df = df.dropna(subset=['gameweek'])
        
        # Convert date strings to proper dates
        if 'match_date' in df.columns:
            df['match_date'] = pd.to_datetime(df['match_date'], errors='coerce')
        
        # Clean and parse time strings
        if 'match_time' in df.columns:
            df['match_time'] = df['match_time'].apply(self._parse_time)
        
        # Parse scores into home_score and away_score
        if 'score' in df.columns:
            df = self._parse_scores(df)
        
        # Determine match completion status
        df['is_completed'] = df['score'].notna() & (df['score'] != '') & (~df['score'].astype(str).str.contains('Head-to-Head|Notes', na=False))
        
        # Clean numeric columns
        numeric_columns = ['home_xg', 'away_xg', 'attendance']
        for col in numeric_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        # Create unique fixture IDs
        df['fixture_id'] = df.apply(self._create_fixture_id, axis=1)
        
        # Add scraping metadata
        df['scraped_date'] = datetime.now().date()
        
        # Drop rows with no teams (empty rows or header rows)
        df = df.dropna(subset=['home_team', 'away_team'])
        
        # Remove any remaining rows where teams contain header-like text
        df = df[~df['home_team'].astype(str).str.contains('Home|Team', case=False, na=False)]
        df = df[~df['away_team'].astype(str).str.contains('Away|Team', case=False, na=False)]
        
        return df
    
    def _parse_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Parse score column into separate home_score and away_score
        """
        def parse_score(score_str):
            if pd.isna(score_str) or score_str == '' or 'Head-to-Head' in str(score_str):
                return None, None
            
            try:
                # Handle formats like "4–2", "1-0", etc.
                score_str = str(score_str).replace('–', '-').replace('—', '-')
                if '-' in score_str:
                    parts = score_str.split('-')
                    if len(parts) == 2:
                        return int(parts[0].strip()), int(parts[1].strip())
            except:
                pass
            
            return None, None
        
        df[['home_score', 'away_score']] = df['score'].apply(lambda x: pd.Series(parse_score(x)))
        
        return df
    
    def _create_fixture_id(self, row) -> str:
        """
        Create unique fixture ID from gameweek, home team, away team
        """
        try:
            gw = int(row['gameweek']) if pd.notna(row['gameweek']) else 0
            home = str(row['home_team']).replace(' ', '')[:10]
            away = str(row['away_team']).replace(' ', '')[:10]
            return f"GW{gw}_{home}_vs_{away}"
        except:
            return f"UNKNOWN_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    def _extract_gameweeks(self, fixtures_df: pd.DataFrame) -> pd.DataFrame:
        """
        Extract gameweek metadata from fixtures
        """
        if 'gameweek' not in fixtures_df.columns:
            logger.warning("No gameweek column found")
            return pd.DataFrame()
        
        # Group by gameweek to get date ranges
        gameweek_data = []
        
        for gw in fixtures_df['gameweek'].dropna().unique():
            gw_fixtures = fixtures_df[fixtures_df['gameweek'] == gw]
            
            # Get date range for this gameweek
            dates = gw_fixtures['match_date'].dropna()
            if len(dates) > 0:
                start_date = dates.min().date() if not dates.empty else None
                end_date = dates.max().date() if not dates.empty else None
            else:
                start_date = None
                end_date = None
            
            # Check if gameweek is complete (all matches have scores)
            completed_matches = gw_fixtures['is_completed'].sum()
            total_matches = len(gw_fixtures)
            is_complete = completed_matches == total_matches and total_matches > 0
            
            gameweek_data.append({
                'gameweek': int(gw),
                'start_date': start_date,
                'end_date': end_date,
                'total_matches': total_matches,
                'completed_matches': completed_matches,
                'is_complete': is_complete
            })
        
        gameweeks_df = pd.DataFrame(gameweek_data)
        gameweeks_df = gameweeks_df.sort_values('gameweek')
        
        return gameweeks_df
    
    def _determine_current_gameweek(self, fixtures_df: pd.DataFrame) -> int:
        """
        Determine current gameweek based on match completion
        Current gameweek = last incomplete gameweek, or next gameweek after last complete one
        """
        if 'gameweek' not in fixtures_df.columns:
            return 1
        
        gameweeks = fixtures_df['gameweek'].dropna().unique()
        gameweeks = sorted([int(gw) for gw in gameweeks])
        
        for gw in gameweeks:
            gw_fixtures = fixtures_df[fixtures_df['gameweek'] == gw]
            incomplete_matches = gw_fixtures['is_completed'] == False
            
            if incomplete_matches.any():  # This gameweek has incomplete matches
                return int(gw)
        
        # If all gameweeks are complete, return the next one
        return max(gameweeks) + 1 if gameweeks else 1
    
    def _extract_season_info(self, table: pd.DataFrame) -> str:
        """
        Extract season information (like '2025-2026') from table or assume current
        """
        # This could be enhanced to parse from page title or other elements
        # For now, return a reasonable default based on current date
        current_year = datetime.now().year
        if datetime.now().month >= 8:  # Season starts in August
            return f"{current_year}-{current_year + 1}"
        else:
            return f"{current_year - 1}-{current_year}"
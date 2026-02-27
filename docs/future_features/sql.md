He sorted stocks into quintiles by valuation multiple across the entire universe then tracked the forward returns of each quintile over one, three, and five year holding periods with annual rebalancing.
The specific mechanics were straightforward but the details matter.
The universe
He used all stocks listed on NYSE, AMEX, and the larger NASDAQ names. He applied a minimum size filter to exclude the very smallest companies where trading costs would make the strategy impractical. In his original work this was roughly the largest 1,500 companies by market cap at any given time. In later editions he expanded this as markets grew.
The sorting methodology
Take the universe on December 31 of each year. Compute the valuation metric — PE, PB, PCF, or dividend yield — for every company. Rank all companies by that metric. Divide into five equal groups. Quintile 1 is the cheapest 20% by that metric. Quintile 5 is the most expensive 20%.
He was careful about a few things most people miss. He excluded financial companies from PCF analysis because cash flow means something different for banks. He excluded companies with negative earnings from PE sorts because a very negative PE is not cheap, it's distressed. He handled these by either excluding them or placing them in a separate category rather than letting them contaminate the quintile boundaries.
The return measurement
Buy the quintile portfolio on January 1. Hold for one year, three years, or five years with no trading. Measure the equally-weighted return of the portfolio. Rebalance annually — a stock that was in Q1 last year might be in Q3 this year if its price has risen relative to its fundamentals.
The equally-weighted approach was deliberate. He wanted to show that the effect wasn't driven by a few large winners. Every stock in the quintile contributed equally to the return regardless of market cap.
What he found
Q1 outperformed Q5 consistently across all four metrics over all holding periods from 1970 through his data cutoff. The spread between Q1 and Q5 widened with longer holding periods — the effect was stronger at five years than one year, which supported his behavioral explanation. Cheap stocks need time for the market to recognize their value.
PCF was his strongest finding. Price to cash flow sorted more reliably than PE because cash flow is harder to manipulate than earnings. A company can accelerate revenue recognition or defer expenses to hit an earnings target. Cash is harder to fake. That's the fundamental insight behind preferring PCF to PE and it maps directly to why you use NCFO in your quality framework.
The spread between Q1 and Q5 on PCF was roughly 6-8 percentage points annually on average in his data. Q1 returned around 14-16% annually, Q5 returned around 8-10%, with the broad market somewhere in between.
The earnings surprise extension
The most interesting part beyond the basic quintile analysis was what he did with earnings surprises. He showed that when a Q1 (cheap, contrarian) stock reported a positive earnings surprise the market reaction was stronger than when a Q5 (glamour) stock reported the same magnitude surprise. And when a Q5 stock reported a negative surprise the market punished it more harshly than a Q1 stock receiving the same negative surprise.
The interpretation is behavioral. Analysts and investors extrapolate recent trends. They expect glamour companies to keep growing and cheap companies to keep struggling. When reality contradicts those expectations the surprise effect is asymmetric — cheap stocks have more room to exceed low expectations, expensive stocks have more room to disappoint high expectations.
This is the mechanism behind the whole contrarian thesis and it's what your regime model is partly detecting. A Q1 stock transitioning from a distribution or stress regime to an accumulation regime is often the early price reflection of expectations beginning to normalize before the fundamental surprise becomes visible.
How to replicate it on your platform
The SQL is clean because you've already built everything it needs.
sqlBACKTEST (
  start_date      = '1995-01-01',
  end_date        = '2024-01-01',
  rebalance       = 'annually',
  weighting       = 'equal',
  min_marketcap   = 'small',
  exclude_sectors = 'Financials'
)
SELECT
  ticker,
  NTILE(5) OVER (
    PARTITION BY date
    ORDER BY evebitda ASC
  ) AS pcf_quintile
FROM fundamentals f
JOIN stocks s USING (ticker)
WHERE available_date = $rebalance_date
  AND evebitda > 0
  AND evebitda < 100          -- exclude extreme outliers
  AND days_since_filing < 120
  AND s.marketcap_tier NOT IN ('Nano', 'Micro')
The PARTITION BY date means quintile assignment is relative to the full universe on each rebalance date, exactly as Dreman did it. NTILE(5) handles the equal-sized bucket creation. The filters replicate his universe construction decisions.
The extension that updates his work
The question worth answering on your platform is whether the PCF effect still holds post-2010 and whether conditioning on your quality metrics improves it. Dreman couldn't answer those questions. You can.
sql-- Dreman PCF but restricted to high quality compounders
-- Does quality filter improve the contrarian effect?
BACKTEST (
  start_date = '2010-01-01',
  end_date   = '2024-01-01',
  rebalance  = 'annually',
  weighting  = 'equal'
)
SELECT
  ticker,
  NTILE(5) OVER (
    PARTITION BY date
    ORDER BY evebitda ASC
  ) AS pcf_quintile
FROM fundamentals f
JOIN stocks s USING (ticker)
WHERE available_date = $rebalance_date
  AND evebitda > 0
  AND evebitda < 100
  AND days_since_filing < 120
  AND ncfo_r2_10y > 0.70      -- Dreman filter + quality filter
  AND roic_level > 0.15
  AND s.marketcap_tier NOT IN ('Nano', 'Micro')
That experiment — does adding quality criteria to Dreman's PCF sort improve returns — is the kind of incremental research that builds directly on the classic work and is genuinely novel. Nobody has run that on a PIT-correct dataset with modern data. Your platform is the first place it can be done cleanly.
That's the experiment you publish first on the Substack. It validates the platform, demonstrates the methodology, and extends the literature in a way that serious investors find immediately interesting.can you give me a detailed description of the sql engine we would need to run dremans experiment like this-- Dreman PCF but restricted to high quality compounders -- Does quality filter improve the contrarian effect? BACKTEST ( start_date = '2010-01-01', end_date = '2024-01-01', rebalance = 'annually', weighting = 'equal' ) SELECT ticker, NTILE(5) OVER ( PARTITION BY date ORDER BY evebitda ASC ) AS pcf_quintile FROM fundamentals f JOIN stocks s USING (ticker) WHERE available_date = $rebalance_date AND evebitda > 0 AND evebitda < 100 AND days_since_filing < 120 AND ncfo_r2_10y > 0.70 -- Dreman filter + quality filter AND roic_level > 0.15 AND s.marketcap_tier NOT IN ('Nano', 'Micro')9:30 PMThis is a custom SQL dialect that extends standard SQL with a backtesting block. The engine needs to parse it, execute the strategy logic across time, and return performance results. Here is exactly what needs to be built.
The overall architecture
The engine has four distinct layers. The parser turns the raw string into a structured object. The planner figures out what needs to be computed and when. The executor runs the walk-forward loop against DuckDB. The performance calculator turns the portfolio returns into statistics.
Raw SQL string
      ↓
   Parser          → BacktestAST + SelectAST
      ↓
   Planner         → execution plan with rebalance dates
      ↓
   Executor        → portfolio weights per rebalance date
      ↓
   Performance     → returns, Sharpe, drawdown, quintile stats
      ↓
   Results object  → JSON to frontend
The parser
The BACKTEST block is not standard SQL so you parse it separately before handing the inner SELECT to sqlglot. The structure is always the same — BACKTEST with named parameters in parentheses, followed by a standard SELECT statement.
python# query/parser.py
import re
import sqlglot
from dataclasses import dataclass, field
from typing import Optional

@dataclass
class BacktestParams:
    start_date: str
    end_date: str
    rebalance: str           # 'annually', 'quarterly', 'monthly'
    weighting: str           # 'equal', 'value', 'rank'
    min_marketcap: Optional[str] = None
    max_positions: Optional[int] = None
    exclude_sectors: Optional[list] = field(default_factory=list)
    benchmark: str = 'SPY'

@dataclass  
class BacktestQuery:
    params: BacktestParams
    select_sql: str          # the inner SELECT, cleaned
    raw: str                 # original for display

class QueryParser:
    
    BACKTEST_PATTERN = re.compile(
        r'BACKTEST\s*\((.*?)\)\s*(SELECT.*)',
        re.DOTALL | re.IGNORECASE
    )
    
    PARAM_PATTERN = re.compile(
        r"(\w+)\s*=\s*'([^']*)'|(\w+)\s*=\s*(\d+)",
        re.IGNORECASE
    )
    
    def parse(self, raw_sql: str) -> BacktestQuery:
        # Strip comments
        sql = self._strip_comments(raw_sql)
        
        match = self.BACKTEST_PATTERN.search(sql)
        if not match:
            raise QueryError(
                "Query must begin with BACKTEST(...) block. "
                "See documentation for syntax."
            )
        
        params_str = match.group(1)
        select_sql = match.group(2).strip()
        
        params = self._parse_params(params_str)
        
        # Validate the inner SELECT with sqlglot
        try:
            ast = sqlglot.parse_one(select_sql)
        except sqlglot.errors.ParseError as e:
            raise QueryError(f"Invalid SQL in SELECT clause: {e}")
        
        # Security validation
        self._validate_select(ast)
        
        return BacktestQuery(
            params=params,
            select_sql=select_sql,
            raw=raw_sql
        )
    
    def _parse_params(self, params_str: str) -> BacktestParams:
        params = {}
        for match in self.PARAM_PATTERN.finditer(params_str):
            if match.group(1):
                key = match.group(1).lower()
                val = match.group(2)
            else:
                key = match.group(3).lower()
                val = int(match.group(4))
            params[key] = val
        
        required = ['start_date', 'end_date', 'rebalance', 'weighting']
        for r in required:
            if r not in params:
                raise QueryError(f"BACKTEST block missing required parameter: {r}")
        
        # Parse exclude_sectors as comma-separated
        if 'exclude_sectors' in params:
            params['exclude_sectors'] = [
                s.strip() for s in params['exclude_sectors'].split(',')
            ]
        
        return BacktestParams(**{
            k: v for k, v in params.items() 
            if k in BacktestParams.__dataclass_fields__
        })
    
    def _validate_select(self, ast):
        """Security validation - same as query compiler"""
        forbidden = [
            sqlglot.expressions.Insert,
            sqlglot.expressions.Update, 
            sqlglot.expressions.Delete,
            sqlglot.expressions.Drop,
            sqlglot.expressions.Create,
            sqlglot.expressions.Command,
        ]
        for node in ast.walk():
            if isinstance(node, tuple(forbidden)):
                raise SecurityError("Write operations not permitted")
        
        # Check table references
        allowed_tables = {
            'fundamentals', 'stocks', 'prices', 
            'regimes', 'macro', 'insider'
        }
        for table in ast.find_all(sqlglot.expressions.Table):
            if table.name.lower() not in allowed_tables:
                raise SecurityError(
                    f"Table '{table.name}' is not accessible. "
                    f"Available: {', '.join(sorted(allowed_tables))}"
                )
    
    def _strip_comments(self, sql: str) -> str:
        # Remove -- comments
        sql = re.sub(r'--[^\n]*', '', sql)
        # Remove /* */ comments  
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        return sql.strip()
The planner
The planner takes the parsed query and figures out the rebalance dates, what the $rebalance_date variable means at each step, and how to inject the BACKTEST parameters as constraints into the user's SELECT.
python# query/planner.py
import pandas as pd
from pandas.tseries.offsets import BYearEnd, BQuarterEnd, BMonthEnd

class QueryPlanner:
    
    REBALANCE_OFFSETS = {
        'annually':   'BA-DEC',   # business year end December
        'quarterly':  'BQ-DEC',   # business quarter end
        'monthly':    'BM',        # business month end
        'semiannual': 'BQ-JUN',   # twice a year
    }
    
    def plan(self, query: BacktestQuery) -> ExecutionPlan:
        rebalance_dates = self._get_rebalance_dates(
            query.params.start_date,
            query.params.end_date,
            query.params.rebalance
        )
        
        # For each rebalance date we need the next rebalance date
        # to know when to measure forward returns
        holding_periods = list(zip(
            rebalance_dates,
            rebalance_dates[1:] + [query.params.end_date]
        ))
        
        # Rewrite the user's SQL to inject virtual table mappings
        # and add BACKTEST parameter constraints
        compiled_sql_template = self._compile_select(
            query.select_sql,
            query.params
        )
        
        return ExecutionPlan(
            params=query.params,
            rebalance_dates=rebalance_dates,
            holding_periods=holding_periods,
            sql_template=compiled_sql_template
        )
    
    def _get_rebalance_dates(
        self, 
        start: str, 
        end: str, 
        frequency: str
    ) -> list:
        offset = self.REBALANCE_OFFSETS.get(frequency)
        if not offset:
            raise QueryError(
                f"Unknown rebalance frequency: {frequency}. "
                f"Valid options: {list(self.REBALANCE_OFFSETS.keys())}"
            )
        
        dates = pd.date_range(
            start=start,
            end=end,
            freq=offset
        )
        
        return [d.strftime('%Y-%m-%d') for d in dates]
    
    def _compile_select(
        self, 
        select_sql: str, 
        params: BacktestParams
    ) -> str:
        """
        Rewrite virtual table names to actual parquet views
        and inject additional WHERE constraints from BACKTEST params.
        
        The $rebalance_date placeholder gets substituted at execution time.
        """
        import sqlglot
        from sqlglot import expressions as exp
        
        ast = sqlglot.parse_one(select_sql)
        
        # Map virtual tables to actual views
        TABLE_MAP = {
            'fundamentals': 'v_fundamentals_pit',
            'stocks':       'v_stocks',
            'prices':       'v_prices',
            'regimes':      'v_regimes',
            'macro':        'v_macro',
            'insider':      'v_insider',
        }
        
        for table in ast.find_all(exp.Table):
            if table.name.lower() in TABLE_MAP:
                table.set(
                    'this',
                    exp.Identifier(this=TABLE_MAP[table.name.lower()])
                )
        
        # Inject sector exclusion if specified
        if params.exclude_sectors:
            sectors_list = ', '.join(
                f"'{s}'" for s in params.exclude_sectors
            )
            # Add to WHERE clause
            extra_where = f"s.sector NOT IN ({sectors_list})"
            # This is simplified - full implementation modifies AST
        
        return ast.sql(dialect='duckdb')
The executor
This is the core of the engine. It walks forward through time, runs the user's SELECT on each rebalance date to get the quintile assignments, then tracks returns of each quintile until the next rebalance.
python# query/executor.py
import duckdb
import pandas as pd
import numpy as np
from pathlib import Path

class BacktestExecutor:
    
    DATA_PATH = Path('outputs')
    
    def __init__(self):
        self.conn = self._setup_connection()
    
    def _setup_connection(self) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect()
        
        # Register parquet files as views
        views = {
            'v_fundamentals_pit': 'features/fundamental_pit.parquet',
            'v_stocks':           'universe/daily_universe.parquet',
            'v_prices':           'features/price_features.parquet',
            'v_regimes':          'models/regime_labels.parquet',
            'v_macro':            'features/macro_features.parquet',
            'v_insider':          'features/insider_institutional.parquet',
        }
        
        for view_name, parquet_path in views.items():
            full_path = self.DATA_PATH / parquet_path
            if full_path.exists():
                conn.execute(f"""
                    CREATE VIEW {view_name} AS 
                    SELECT * FROM read_parquet('{full_path}')
                """)
        
        # Set resource limits
        conn.execute("SET memory_limit='2GB'")
        conn.execute("SET threads=4")
        
        return conn
    
    def execute(self, plan: ExecutionPlan) -> BacktestResults:
        
        quintile_portfolios = []  # list of {date, ticker, quintile}
        
        for rebalance_date, next_rebalance_date in plan.holding_periods:
            
            # Substitute $rebalance_date into template
            sql = plan.sql_template.replace(
                '$rebalance_date', 
                f"'{rebalance_date}'"
            )
            
            try:
                # Get quintile assignments on this rebalance date
                assignments = self.conn.execute(sql).df()
                
                if len(assignments) == 0:
                    continue
                
                # Validate output has required columns
                self._validate_output(assignments)
                
                # Add metadata
                assignments['rebalance_date'] = rebalance_date
                assignments['next_rebalance_date'] = next_rebalance_date
                
                quintile_portfolios.append(assignments)
                
            except duckdb.Error as e:
                raise ExecutionError(
                    f"Query failed on date {rebalance_date}: {e}"
                )
        
        if not quintile_portfolios:
            raise ExecutionError("No data returned for any rebalance date")
        
        all_assignments = pd.concat(quintile_portfolios, ignore_index=True)
        
        # Calculate returns for each portfolio
        returns = self._calculate_returns(all_assignments, plan.params)
        
        return returns
    
    def _validate_output(self, df: pd.DataFrame):
        """
        The user's SELECT must return at minimum:
        - ticker (to look up returns)
        - a quintile column (NTILE output or similar ranking)
        """
        if 'ticker' not in df.columns:
            raise QueryError(
                "SELECT must include 'ticker' column"
            )
        
        # Find the quintile/ranking column
        # Look for NTILE output - any integer column that isn't ticker
        rank_cols = [
            c for c in df.columns 
            if c != 'ticker' and df[c].dtype in ['int64', 'int32', 'float64']
        ]
        
        if not rank_cols:
            raise QueryError(
                "SELECT must include a ranking column. "
                "Use NTILE(5) OVER (...) AS quintile or similar."
            )
    
    def _calculate_returns(
        self, 
        assignments: pd.DataFrame,
        params: BacktestParams
    ) -> 'BacktestResults':
        """
        For each ticker on each rebalance date, look up the return
        from rebalance_date to next_rebalance_date.
        Then aggregate by quintile.
        """
        
        # Find the quintile column name
        quintile_col = [
            c for c in assignments.columns 
            if c not in ('ticker', 'rebalance_date', 'next_rebalance_date')
        ][0]
        
        # Load price data for return calculation
        # Get all tickers we need returns for
        tickers = assignments.ticker.unique().tolist()
        ticker_list = ', '.join(f"'{t}'" for t in tickers)
        
        prices = self.conn.execute(f"""
            SELECT ticker, date, closeadj
            FROM read_parquet('{self.DATA_PATH}/sep/sep.parquet')
            WHERE ticker IN ({ticker_list})
            AND date BETWEEN '{params.start_date}' AND '{params.end_date}'
            ORDER BY ticker, date
        """).df()
        
        # Calculate holding period returns for each assignment
        results = []
        
        for _, row in assignments.iterrows():
            ticker = row['ticker']
            entry_date = row['rebalance_date']
            exit_date = row['next_rebalance_date']
            quintile = row[quintile_col]
            
            ticker_prices = prices[prices.ticker == ticker].copy()
            ticker_prices = ticker_prices.set_index('date').sort_index()
            
            # Find closest available price on or after entry date
            entry_price = self._get_price_on_or_after(
                ticker_prices, entry_date
            )
            
            # Find closest available price on or before exit date
            exit_price = self._get_price_on_or_before(
                ticker_prices, exit_date
            )
            
            if entry_price is None or exit_price is None:
                continue
            
            # Handle delisting - if ticker was delisted during holding period
            # use last available price (already handled by using on_or_before)
            holding_return = (exit_price / entry_price) - 1
            
            results.append({
                'rebalance_date': entry_date,
                'ticker': ticker,
                'quintile': int(quintile),
                'entry_price': entry_price,
                'exit_price': exit_price,
                'holding_return': holding_return,
            })
        
        results_df = pd.DataFrame(results)
        
        return self._compute_statistics(results_df, params)
    
    def _get_price_on_or_after(
        self, 
        prices: pd.DataFrame, 
        date: str
    ) -> Optional[float]:
        """Get first available price on or after date"""
        mask = prices.index >= date
        if not mask.any():
            return None
        return prices.loc[mask, 'closeadj'].iloc[0]
    
    def _get_price_on_or_before(
        self,
        prices: pd.DataFrame,
        date: str
    ) -> Optional[float]:
        """Get last available price on or before date"""
        mask = prices.index <= date
        if not mask.any():
            return None
        return prices.loc[mask, 'closeadj'].iloc[-1]
    
    def _compute_statistics(
        self,
        results: pd.DataFrame,
        params: BacktestParams
    ) -> 'BacktestResults':
        
        quintile_stats = []
        
        for q in sorted(results.quintile.unique()):
            q_returns = results[results.quintile == q]['holding_return']
            
            # Equal weight within quintile
            mean_return = q_returns.mean()
            std_return = q_returns.std()
            
            # Annualize based on rebalance frequency
            periods_per_year = {
                'annually': 1,
                'quarterly': 4,
                'monthly': 12,
            }[params.rebalance]
            
            ann_return = (1 + mean_return) ** periods_per_year - 1
            ann_std = std_return * np.sqrt(periods_per_year)
            sharpe = ann_return / ann_std if ann_std > 0 else 0
            
            quintile_stats.append({
                'quintile': q,
                'mean_return': mean_return,
                'ann_return': ann_return,
                'ann_std': ann_std,
                'sharpe': sharpe,
                'n_obs': len(q_returns),
                'win_rate': (q_returns > 0).mean(),
                'median_return': q_returns.median(),
                'worst': q_returns.quantile(0.05),
                'best': q_returns.quantile(0.95),
            })
        
        # Time series of quintile returns for cumulative chart
        timeseries = self._compute_timeseries(results, params)
        
        # Statistical significance of Q1 vs Q5 spread
        q1_returns = results[results.quintile == 1]['holding_return']
        q5_returns = results[results.quintile == 5]['holding_return']
        
        from scipy import stats
        t_stat, p_value = stats.ttest_ind(q1_returns, q5_returns)
        
        return BacktestResults(
            quintile_stats=pd.DataFrame(quintile_stats),
            timeseries=timeseries,
            q1_vs_q5_spread=q1_returns.mean() - q5_returns.mean(),
            t_statistic=t_stat,
            p_value=p_value,
            n_rebalance_periods=results.rebalance_date.nunique(),
            n_total_observations=len(results),
            params=params,
        )
    
    def _compute_timeseries(
        self,
        results: pd.DataFrame,
        params: BacktestParams
    ) -> pd.DataFrame:
        """
        Compute cumulative return of each quintile over time.
        Used for the cumulative returns chart.
        """
        timeseries = []
        
        for q in sorted(results.quintile.unique()):
            q_data = results[results.quintile == q].copy()
            q_data = q_data.sort_values('rebalance_date')
            
            # Equal weight portfolio return per period
            period_returns = q_data.groupby('rebalance_date')['holding_return'].mean()
            
            # Cumulative return
            cumulative = (1 + period_returns).cumprod()
            
            for date, value in cumulative.items():
                timeseries.append({
                    'date': date,
                    'quintile': q,
                    'cumulative_return': value - 1
                })
        
        return pd.DataFrame(timeseries)
The performance calculator extension for regime breakdown
Once you have the regime model running you extend the results with regime-conditional statistics by joining regime labels to each holding period.
pythondef _compute_regime_stats(
    self,
    results: pd.DataFrame
) -> pd.DataFrame:
    """
    For each ticker-period in results, look up what regime
    the stock was in on the rebalance date.
    Then show return statistics by quintile AND regime.
    """
    # Load regime labels
    regimes = self.conn.execute(f"""
        SELECT ticker, date, regime
        FROM read_parquet('{self.DATA_PATH}/models/regime_labels.parquet')
    """).df()
    
    # Join regime to results on ticker + rebalance_date
    results_with_regime = results.merge(
        regimes.rename(columns={'date': 'rebalance_date'}),
        on=['ticker', 'rebalance_date'],
        how='left'
    )
    
    # Stats by quintile and regime
    regime_stats = results_with_regime.groupby(
        ['quintile', 'regime']
    )['holding_return'].agg([
        'mean', 'std', 'count'
    ]).reset_index()
    
    return regime_stats
The results dataclass and API response
python# query/results.py
from dataclasses import dataclass
import pandas as pd
import json

@dataclass
class BacktestResults:
    quintile_stats: pd.DataFrame
    timeseries: pd.DataFrame
    q1_vs_q5_spread: float
    t_statistic: float
    p_value: float
    n_rebalance_periods: int
    n_total_observations: int
    params: BacktestParams
    regime_stats: pd.DataFrame = None
    
    def to_json(self) -> dict:
        return {
            'quintile_stats': self.quintile_stats.to_dict(orient='records'),
            'timeseries': self.timeseries.to_dict(orient='records'),
            'summary': {
                'q1_vs_q5_spread': round(self.q1_vs_q5_spread * 100, 2),
                't_statistic': round(self.t_statistic, 3),
                'p_value': round(self.p_value, 4),
                'is_significant': self.p_value < 0.05,
                'n_periods': self.n_rebalance_periods,
                'n_observations': self.n_total_observations,
            },
            'regime_stats': (
                self.regime_stats.to_dict(orient='records') 
                if self.regime_stats is not None else None
            ),
            'params': {
                'start_date': self.params.start_date,
                'end_date': self.params.end_date,
                'rebalance': self.params.rebalance,
                'weighting': self.params.weighting,
            }
        }
The FastAPI endpoint that ties it together
python# routes/experiments.py
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from query.parser import QueryParser, QueryError, SecurityError
from query.planner import QueryPlanner
from query.executor import BacktestExecutor
import asyncio

router = APIRouter()
parser = QueryParser()
planner = QueryPlanner()
executor = BacktestExecutor()

class ExperimentRequest(BaseModel):
    sql: str
    user_id: str
    tier: str = 'investor'

@router.post("/api/experiments/run")
async def run_experiment(request: ExperimentRequest):
    
    try:
        # Parse
        query = parser.parse(request.sql)
        
        # Check date range limits by tier
        _enforce_date_limits(query.params, request.tier)
        
        # Plan
        plan = planner.plan(query)
        
        # Execute with timeout
        results = await asyncio.wait_for(
            asyncio.to_thread(executor.execute, plan),
            timeout=120  # 2 minute limit
        )
        
        return results.to_json()
    
    except SecurityError as e:
        raise HTTPException(status_code=403, detail=str(e))
    except QueryError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except asyncio.TimeoutError:
        raise HTTPException(
            status_code=408,
            detail=(
                "Experiment exceeded time limit. "
                "Try a shorter date range or add more filters."
            )
        )

def _enforce_date_limits(params: BacktestParams, tier: str):
    """Limit historical range by subscription tier"""
    limits = {
        'free':         5,   # 5 years
        'investor':     15,  # 15 years
        'professional': 30,  # full history
    }
    
    years_requested = (
        pd.Timestamp(params.end_date) - 
        pd.Timestamp(params.start_date)
    ).days / 365
    
    max_years = limits.get(tier, 5)
    
    if years_requested > max_years:
        raise QueryError(
            f"Your {tier} plan supports up to {max_years} years of history. "
            f"You requested {years_requested:.0f} years. "
            f"Upgrade for full history access."
        )
The error messages that make it usable
The engine is only as good as its error messages. When something goes wrong the user needs to know exactly what to fix.
python# query/errors.py

HELPFUL_ERRORS = {
    'table_not_found': (
        "Table '{table}' is not accessible. "
        "Available tables: fundamentals, stocks, prices, regimes, macro, insider. "
        "Example: SELECT ticker FROM fundamentals WHERE ..."
    ),
    'missing_ticker': (
        "Your SELECT must include a 'ticker' column so the engine can look up returns. "
        "Add 'ticker' to your SELECT list."
    ),
    'missing_ranking': (
        "Your SELECT needs a ranking column to define quintiles. "
        "Add: NTILE(5) OVER (PARTITION BY date ORDER BY your_metric ASC) AS quintile"
    ),
    'no_rebalance_date': (
        "Your WHERE clause must filter by $rebalance_date for point-in-time correctness. "
        "Add: WHERE available_date = $rebalance_date"
    ),
    'no_data_returned': (
        "Your query returned no stocks on {date}. "
        "Check that your filters aren't too restrictive. "
        "Try removing one filter at a time to see which is excluding all results."
    ),
}
What the engine does not handle yet and needs later
Transaction costs — applying a bid-ask spread and market impact model to each rebalance. Without this the backtest overstates returns especially for smaller names. Short side — the Dreman analysis is long only but eventually you want to show long Q1 short Q5 returns. Factor exposure decomposition — how much of Q1's outperformance is value factor, how much is quality factor, how much is momentum. These are phase two features. The core engine as described handles the Dreman experiment correctly and that's the right scope to build first.

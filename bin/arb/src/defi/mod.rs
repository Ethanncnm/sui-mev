mod aftermath;
mod blue_move;
mod cetus;
mod deepbook_v2;
mod flowx_clmm;
mod indexer_searcher;
mod kriya_amm;
mod kriya_clmm;
mod navi;
mod shio;
mod trade;
mod turbos;
mod utils;

use std::{
    collections::HashSet,
    fmt,
    hash::Hash,
    sync::Arc,
};

use ::utils::coin;
use dex_indexer::types::Protocol;
use eyre::{bail, ensure, Result};
pub use indexer_searcher::IndexerDexSearcher;
use object_pool::ObjectPool;
use simulator::{SimulateCtx, Simulator};
use sui_sdk::SUI_COIN_TYPE;
use sui_types::{
    base_types::{ObjectID, ObjectRef, SuiAddress},
    transaction::{Argument, TransactionData},
};
use tokio::task::JoinSet;
use tracing::Instrument;
use trade::{FlashResult, TradeResult};
pub use trade::{Path, TradeCtx, TradeType, Trader};

use crate::{config::pegged_coin_types, types::Source};

const MAX_HOP_COUNT: usize = 2;
const MAX_POOL_COUNT: usize = 10;
const MIN_LIQUIDITY: u128 = 1000;

pub const CETUS_AGGREGATOR: &str = "0x11451575c775a3e633437b827ecbc1eb51a5964b0302210b28f5b89880be21a2";

#[async_trait::async_trait]
pub trait DexSearcher: Send + Sync {
    // coin_type: e.g. "0x2::sui::SUI"
    async fn find_dexes(&self, coin_in_type: &str, coin_out_type: Option<String>) -> Result<Vec<Box<dyn Dex>>>;

    async fn find_test_path(&self, path: &[ObjectID]) -> Result<Path>;
}

#[async_trait::async_trait]
pub trait Dex: Send + Sync + CloneBoxedDex {
    fn support_flashloan(&self) -> bool {
        false
    }

    /// Extend the trade_tx with a flashloan tx.
    /// Returns (coin_out, receipt).
    async fn extend_flashloan_tx(&self, _ctx: &mut TradeCtx, _amount: u64) -> Result<FlashResult> {
        bail!("flashloan not supported")
    }

    /// Extend the trade_tx with a repay tx.
    /// Returns the coin_profit after repaying the flashloan.
    async fn extend_repay_tx(&self, _ctx: &mut TradeCtx, _coin: Argument, _flash_res: FlashResult) -> Result<Argument> {
        bail!("flashloan not supported")
    }

    /// Extend the trade_tx with a swap tx.
    /// Returns coin_out for the next swap.
    async fn extend_trade_tx(
        &self,
        ctx: &mut TradeCtx,
        sender: SuiAddress,
        coin_in: Argument,
        amount_in: Option<u64>,
    ) -> Result<Argument>;

    fn coin_in_type(&self) -> String;
    fn coin_out_type(&self) -> String;
    fn protocol(&self) -> Protocol;
    fn liquidity(&self) -> u128;
    fn object_id(&self) -> ObjectID;

    /// flip the coin_in_type and coin_out_type
    fn flip(&mut self);

    // for debug
    fn is_a2b(&self) -> bool;
    async fn swap_tx(&self, sender: SuiAddress, recipient: SuiAddress, amount_in: u64) -> Result<TransactionData>;
}

pub trait CloneBoxedDex {
    fn clone_boxed(&self) -> Box<dyn Dex>;
}

impl<T> CloneBoxedDex for T
where
    T: 'static + Dex + Clone,
{
    fn clone_boxed(&self) -> Box<dyn Dex> {
        Box::new(self.clone())
    }
}

impl Clone for Box<dyn Dex> {
    fn clone(&self) -> Box<dyn Dex> {
        self.clone_boxed()
    }
}

impl PartialEq for Box<dyn Dex> {
    fn eq(&self, other: &Self) -> bool {
        self.object_id() == other.object_id()
    }
}

impl Eq for Box<dyn Dex> {}

impl Hash for Box<dyn Dex> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.object_id().hash(state);
    }
}

impl fmt::Debug for Box<dyn Dex> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{}({}, {}, {})",
            self.protocol(),
            self.object_id(),
            self.coin_in_type(),
            self.coin_out_type()
        )
    }
}

#[derive(Clone)]
pub struct Defi {
    dex_searcher: Arc<dyn DexSearcher>,
    trader: Arc<Trader>,
}

impl Defi {
    pub async fn new(http_url: &str, simulator_pool: Arc<ObjectPool<Box<dyn Simulator>>>) -> Result<Self> {
        let dex_searcher = IndexerDexSearcher::new(http_url, simulator_pool.clone()).await?;
        let trade = Trader::new(simulator_pool).await?;

        Ok(Self {
            dex_searcher: Arc::new(dex_searcher),
            trader: Arc::new(trade),
        })
    }

    #[allow(dead_code)]
    pub async fn find_dexes(&self, coin_in_type: &str, coin_out_type: Option<String>) -> Result<Vec<Box<dyn Dex>>> {
        self.dex_searcher.find_dexes(coin_in_type, coin_out_type).await
    }

    pub async fn find_sell_paths(&self, coin_in_type: &str) -> Result<Vec<Path>> {
        if coin::is_native_coin(coin_in_type) {
            return Ok(vec![Path::default()]);
        }

        #[derive(Clone)]
        struct SearchPath {
            path: Vec<Box<dyn Dex>>,
            next_coin: String,
            min_liquidity: u128,
        }

        let mut queue = vec![SearchPath {
            path: vec![],
            next_coin: coin_in_type.to_string(),
            min_liquidity: u128::MAX,
        }];
        let mut visited_dexes = HashSet::new();
        let mut results = vec![];

        for hop in 0..MAX_HOP_COUNT {
            let is_last = hop == MAX_HOP_COUNT - 1;
            let mut next_queue = Vec::new();

            for search_path in queue.into_iter() {
                let coin_type = search_path.next_coin.clone();
                if coin::is_native_coin(&coin_type) {
                    results.push(search_path.path);
                    continue;
                }

                let coin_out_type = if pegged_coin_types().contains(coin_type.as_str()) || is_last {
                    Some(SUI_COIN_TYPE.to_string())
                } else {
                    None
                };
                let mut dexes = if let Ok(dexes) = self.dex_searcher.find_dexes(&coin_type, coin_out_type).await {
                    dexes
                } else {
                    continue;
                };

                dexes.retain(|d| d.liquidity() >= MIN_LIQUIDITY);
                if dexes.len() > MAX_POOL_COUNT {
                    dexes.retain(|d| !visited_dexes.contains(&d.object_id()));
                    dexes.sort_by_key(|d| std::cmp::Reverse(d.liquidity()));
                    dexes.truncate(MAX_POOL_COUNT);
                }

                for dex in dexes {
                    if visited_dexes.contains(&dex.object_id()) {
                        continue;
                    }
                    visited_dexes.insert(dex.object_id());
                    let mut new_path = search_path.path.clone();
                    new_path.push(dex.clone());
                    let next_coin = dex.coin_out_type();
                    let min_liq = search_path.min_liquidity.min(dex.liquidity());
                    next_queue.push(SearchPath { path: new_path, next_coin, min_liquidity: min_liq });
                }
            }

            queue = next_queue;
        }

        for sp in queue {
            if coin::is_native_coin(&sp.next_coin) {
                results.push(sp.path);
            }
        }

        results.sort_by_key(|path| {
            let min_liq = path.iter().map(|d| d.liquidity()).min().unwrap_or(0);
            std::cmp::Reverse(min_liq)
        });

        Ok(results.into_iter().map(Path::new).collect())
    }

    pub async fn find_buy_paths(&self, coin_out_type: &str) -> Result<Vec<Path>> {
        let mut paths = self.find_sell_paths(coin_out_type).await?;
        for path in &mut paths {
            path.path.reverse();
            for dex in &mut path.path {
                dex.flip();
            }
        }

        Ok(paths)
    }

    pub async fn find_best_path_exact_in(
        &self,
        paths: &[Path],
        sender: SuiAddress,
        amount_in: u64,
        trade_type: TradeType,
        gas_coins: &[ObjectRef],
        sim_ctx: &SimulateCtx,
    ) -> Result<PathTradeResult> {
        let mut joinset = JoinSet::new();

        for (idx, path) in paths.iter().enumerate() {
            if path.is_empty() {
                continue;
            }

            let trade = self.trader.clone();
            let path = path.clone();
            let gas_coins = gas_coins.to_vec();
            let sim_ctx = sim_ctx.clone();

            joinset.spawn(
                async move {
                    let result = trade
                        .get_trade_result(&path, sender, amount_in, trade_type, gas_coins, sim_ctx)
                        .await;

                    (idx, result)
                }
                .in_current_span(),
            );
        }

        let (mut best_idx, mut best_trade_res) = (0, TradeResult::default());
        while let Some(Ok((idx, trade_res))) = joinset.join_next().await {
            match trade_res {
                Ok(trade_res) => {
                    if trade_res > best_trade_res {
                        best_idx = idx;
                        best_trade_res = trade_res;
                    }
                }
                Err(_error) => {
                    // tracing::error!(path = ?paths[idx], ?error, "trade
                    // error");
                }
            }
        }

        ensure!(best_trade_res.amount_out > 0, "zero amount_out");

        Ok(PathTradeResult::new(paths[best_idx].clone(), amount_in, best_trade_res))
    }

    pub async fn build_final_tx_data(
        &self,
        sender: SuiAddress,
        amount_in: u64,
        path: &Path,
        gas_coins: Vec<ObjectRef>,
        gas_price: u64,
        source: Source,
    ) -> Result<TransactionData> {
        let (tx_data, _) = self
            .trader
            .get_flashloan_trade_tx(path, sender, amount_in, gas_coins, gas_price, source)
            .await?;

        Ok(tx_data)
    }
}


#[derive(Debug, Clone)]
pub struct PathTradeResult {
    pub path: Path,
    pub amount_in: u64,
    pub amount_out: u64,
    pub gas_cost: i64,
    pub cache_misses: u64,
}

impl PathTradeResult {
    pub fn new(path: Path, amount_in: u64, trade_res: TradeResult) -> Self {
        Self {
            path,
            amount_in,
            amount_out: trade_res.amount_out,
            gas_cost: trade_res.gas_cost,
            cache_misses: trade_res.cache_misses,
        }
    }

    pub fn profit(&self) -> i128 {
        if self.path.coin_in_type() == SUI_COIN_TYPE {
            if self.path.coin_out_type() == SUI_COIN_TYPE {
                return self.amount_out as i128 - self.amount_in as i128 - self.gas_cost as i128;
            }
            0 - self.gas_cost as i128 - self.amount_in as i128
        } else {
            0
        }
    }
}

impl fmt::Display for PathTradeResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PathTradeResult {{ amount_in: {}, amount_out: {}, profit: {}, path: {:?} ... }}",
            self.amount_in,
            self.amount_out,
            self.profit(),
            self.path
        )
    }
}

#[cfg(test)]
mod tests {

    use simulator::HttpSimulator;
    use tracing::info;

    use super::*;
    use crate::config::tests::TEST_HTTP_URL;

    #[tokio::test]
    async fn test_find_sell_paths() {
        mev_logger::init_console_logger_with_directives(None, &["arb=debug", "dex_indexer=debug"]);

        let simulator_pool = ObjectPool::new(1, move || {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(async { Box::new(HttpSimulator::new(&TEST_HTTP_URL, &None).await) as Box<dyn Simulator> })
        });

        let defi = Defi::new(TEST_HTTP_URL, Arc::new(simulator_pool)).await.unwrap();

        let coin_in_type = "0xa8816d3a6e3136e86bc2873b1f94a15cadc8af2703c075f2d546c2ae367f4df9::ocean::OCEAN";
        let paths = defi.find_sell_paths(coin_in_type).await.unwrap();
        assert!(!paths.is_empty(), "No sell paths found");

        for path in paths {
            info!(?path, "sell")
        }
    }

    #[tokio::test]
    async fn test_find_buy_paths() {
        mev_logger::init_console_logger_with_directives(None, &["arb=debug", "dex_indexer=debug"]);

        let simulator_pool = ObjectPool::new(1, move || {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(async { Box::new(HttpSimulator::new(&TEST_HTTP_URL, &None).await) as Box<dyn Simulator> })
        });

        let defi = Defi::new(TEST_HTTP_URL, Arc::new(simulator_pool)).await.unwrap();

        let coin_out_type = "0xa8816d3a6e3136e86bc2873b1f94a15cadc8af2703c075f2d546c2ae367f4df9::ocean::OCEAN";
        let paths = defi.find_buy_paths(coin_out_type).await.unwrap();
        assert!(!paths.is_empty(), "No buy paths found");
        for path in paths {
            info!(?path, "buy")
        }
    }
}

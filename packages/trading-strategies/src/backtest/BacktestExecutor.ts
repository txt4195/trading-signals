import Big from 'big.js';
import {CandleBatcher} from '@typedtrader/exchange';
import type {Candle} from '@typedtrader/exchange';
import {TradingSession} from '../trader/TradingSession.js';
import type {OrderAdvice} from '../trader/TradingSessionTypes.js';
import type {BacktestConfig} from './BacktestConfig.js';
import type {
  BacktestPerformanceSummary,
  BacktestResult,
  BacktestSkippedAdvice,
  BacktestTrade,
} from './BacktestResult.js';
import {PerformanceCalculator} from './PerformanceCalculator.js';

interface PerformanceSummaryConfig {
  candles: Candle[];
  /** Portfolio value at the initial open, followed by one entry per processed candle. */
  equityCurve: readonly Big[];
  finalPortfolioValue: Big;
  initialPortfolioValue: Big;
  trades: BacktestTrade[];
}

export class BacktestExecutor {
  readonly #config: BacktestConfig;

  constructor(config: BacktestConfig) {
    this.#config = config;
  }

  async execute(): Promise<BacktestResult> {
    const {broker: exchange, candles, strategy, tradingPair} = this.#config;

    const initialBalances = await exchange.getAvailableBalances(tradingPair);
    const initialBaseBalance = initialBalances.base;
    const initialCounterBalance = initialBalances.counter;

    /*
     * Same call TradingSession.start() makes before the first candle, with the broker as market.
     * A live session starts "now"; a backtest starts when its first candle opens.
     */
    if (candles[0]) {
      exchange.setStartTime(candles[0].openTimeInISO);
    }
    const session = new TradingSession({broker: exchange, pair: tradingPair, strategy});
    await session.start({candleSource: 'external'});
    const sessionErrors: Error[] = [];
    session.on('error', error => {
      sessionErrors.push(error);
    });

    const trades: BacktestTrade[] = [];
    const skippedAdvices: BacktestSkippedAdvice[] = [];
    let totalFees = new Big(0);
    const firstOpenPrice = candles.length > 0 ? new Big(candles[0].open) : new Big(0);
    const initialPortfolioValue = initialBaseBalance.mul(firstOpenPrice).plus(initialCounterBalance);
    const equityCurve = [initialPortfolioValue];

    try {
      for (const candle of candles) {
        // Step 1: Match pending orders from previous iteration against this candle
        const fills = exchange.processCandle(candle);

        if (fills.length > 0) {
          for (const fill of fills) {
            trades.push({
              advice: this.#findAdviceForFill(fill.order_id),
              fee: new Big(fill.fee),
              openTimeInISO: fill.created_at,
              price: new Big(fill.price),
              side: fill.side,
              size: new Big(fill.size),
            });
            totalFees = totalFees.plus(fill.fee);
          }
        }

        // TradingSession consumes the already-emitted fills, then evaluates this candle and
        // executes advice through the same owner used by live trading.
        const result = await session.next(CandleBatcher.createOneMinuteBatchedCandle([candle]));
        const unreportedError = sessionErrors.find(error =>
          !(result?.execution.status === 'SKIPPED' && result.execution.error === error)
        );
        sessionErrors.length = 0;
        if (unreportedError) throw unreportedError;
        equityCurve.push(await this.#calculatePortfolioValue(new Big(candle.close)));

        if (!result) {
          continue;
        }

        if (result.execution.status === 'PLACED') {
          this.#orderAdviceMap.set(result.execution.order.id, result.advice);
        } else {
          skippedAdvices.push({
            advice: result.advice,
            openTimeInISO: candle.openTimeInISO,
            reason: result.execution.error.message,
          });
        }
      }
    } finally {
      await session.stop({cancelOpenOrders: true});
    }

    /*
     * Cancel any orders placed on the last candle that were never filled.
     * This releases their held balances back to available so the final stats
     * correctly reflect what the portfolio actually holds.
     */
    const finalBalances = await exchange.getAvailableBalances(tradingPair);
    const lastClosePrice = candles.length > 0 ? new Big(candles[candles.length - 1].close) : new Big(0);

    const finalPortfolioValue = finalBalances.base.mul(lastClosePrice).plus(finalBalances.counter);
    const profitOrLoss = finalPortfolioValue.minus(initialPortfolioValue);

    const performance = this.#buildPerformanceSummary({
      candles,
      equityCurve,
      finalPortfolioValue,
      initialPortfolioValue,
      trades,
    });

    return {
      finalBaseBalance: finalBalances.base,
      finalCounterBalance: finalBalances.counter,
      initialBaseBalance,
      initialCounterBalance,
      performance,
      profitOrLoss,
      skippedAdvices,
      totalCandles: candles.length,
      totalFees,
      trades,
    };
  }

  /** Map to track which order_id corresponds to which advice */
  readonly #orderAdviceMap = new Map<string, OrderAdvice>();

  #findAdviceForFill(orderId: string): OrderAdvice {
    const advice = this.#orderAdviceMap.get(orderId);
    if (!advice) {
      // Fabricating a fallback advice here would silently corrupt the cycle/win-rate stats
      throw new Error(`No advice recorded for filled order "${orderId}"`);
    }
    return advice;
  }

  async #calculatePortfolioValue(price: Big): Promise<Big> {
    const {broker, tradingPair} = this.#config;
    const balances = await broker.listBalances();
    const total = (currency: string) => {
      const balance = balances.find(item => item.currency === currency);
      return new Big(balance?.available ?? 0).plus(balance?.hold ?? 0);
    };
    return total(tradingPair.base).mul(price).plus(total(tradingPair.counter));
  }

  #buildPerformanceSummary({
    candles,
    equityCurve,
    finalPortfolioValue,
    initialPortfolioValue,
    trades,
  }: PerformanceSummaryConfig): BacktestPerformanceSummary {
    const returnPercentage = initialPortfolioValue.gt(0)
      ? finalPortfolioValue.minus(initialPortfolioValue).div(initialPortfolioValue).mul(100)
      : new Big(0);

    const winRate = PerformanceCalculator.calculateWinRate(trades);
    const buyAndHoldReturnPercentage = PerformanceCalculator.calculateBuyAndHoldReturn(candles);
    const maxDrawdownPercentage = PerformanceCalculator.calculateMaxDrawdown(equityCurve);
    const sharpeRatio = PerformanceCalculator.calculateSharpeRatio(equityCurve);
    const sortinoRatio = PerformanceCalculator.calculateSortinoRatio(equityCurve);
    const {maxLossStreak, maxWinStreak} = PerformanceCalculator.calculateStreaks(trades);

    return {
      buyAndHoldReturnPercentage,
      finalPortfolioValue,
      initialPortfolioValue,
      maxDrawdownPercentage,
      maxLossStreak,
      maxWinStreak,
      returnPercentage,
      sharpeRatio,
      sortinoRatio,
      totalTrades: trades.length,
      winRate,
    };
  }
}

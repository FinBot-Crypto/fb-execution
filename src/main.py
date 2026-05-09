"""
fb-execution: Executa ordens na Binance.

Fluxo:
  trade.order → para cada ordem:
    → market BUY
    → stop-limit SELL (SL)
    → limit SELL (TP)
    → publica trade.executed
"""
import asyncio, logging, os, json, ccxt, nats
from nats.js.api import ConsumerConfig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("fb-execution")

NATS_URL = os.getenv("NATS_URL", "nats://crypto-nats:4222")
BINANCE_API_KEY = os.getenv("BINANCE_API_KEY", "")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET", "")
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"
MAX_POSITIONS = int(os.getenv("MAX_POSITIONS", "20"))


class ExecutionEngine:
    def __init__(self):
        self.nc = None
        self.js = None
        self.kv = None
        self.exchange = ccxt.binance({
            "apiKey": BINANCE_API_KEY,
            "secret": BINANCE_API_SECRET,
            "enableRateLimit": True,
        })

    async def connect_nats(self):
        self.nc = await nats.connect(NATS_URL)
        self.js = self.nc.jetstream()
        self.kv = await self.js.key_value("active_positions")
        logger.info(f"NATS conectado: {NATS_URL}")

    async def position_exists(self, symbol):
        """Verifica se já tem posição aberta no KV store."""
        try:
            key = symbol.replace("/", "_")
            await self.kv.get(key)
            return True
        except Exception:
            return False

    async def count_positions(self):
        """Conta posições ativas no KV store."""
        try:
            keys = await self.kv.keys()
            return len(keys)
        except Exception:
            return 0

    async def get_open_positions(self):
        """Verifica posições já abertas na conta (da Binance, não mock)."""
        try:
            balance = self.exchange.fetch_balance()
            positions = {}
            for asset, info in balance["total"].items():
                if info > 0 and asset != "USDT":
                    if info * (balance.get(asset, {}).get("USDT", 0) or 1) > 5:
                        positions[asset] = info
            return positions
        except Exception as e:
            logger.error(f"Erro ao verificar posições: {e}")
            return {}

    async def execute_order(self, order):
        symbol = order["symbol"]
        quantity = order["quantity"]
        sl_price = order["sl_price"]
        tp_price = order["tp_price"]
        entry_price = order["entry_price"]

        # Verifica se já tem posição aberta (KV store)
        if await self.position_exists(symbol):
            logger.info(f"  {symbol}: já tem posição aberta → ignora")
            return None

        # Verifica máximo de posições (KV store)
        active_count = await self.count_positions()
        if active_count >= MAX_POSITIONS:
            logger.info(f"  {symbol}: max posições ({MAX_POSITIONS}) atingido → ignora")
            return None

        if DRY_RUN:
            logger.info(f"  [DRY RUN] {symbol}: BUY {quantity} @ ~{entry_price} SL={sl_price} TP={tp_price}")
            pos_data = {"symbol": symbol, "quantity": quantity, "entry_price": entry_price,
                         "sl_price": sl_price, "tp_price": tp_price, "entry_time": __import__('time').time()}
            await self.kv.put(symbol.replace("/", "_"), json.dumps(pos_data).encode())
            return {"symbol": symbol, "status": "dry_run", "quantity": quantity,
                    "entry_price": entry_price, "sl_price": sl_price, "tp_price": tp_price,
                    "tier": order.get("tier"), "strategy": order.get("strategy"),
                    "score": order.get("score"), "rsi": order.get("rsi"),
                    "direction": order.get("direction", "LONG")}

        try:
            logger.info(f"  {symbol}: executando market BUY {quantity}...")
            buy_order = self.exchange.create_order(symbol, "market", "buy", quantity)
            filled_price = float(buy_order.get("average", buy_order.get("price", entry_price)))
            filled_qty = float(buy_order.get("filled", quantity))
            logger.info(f"  {symbol}: BUY executado {filled_qty} @ {filled_price}")

            # Quantidade real no saldo (pós-taxa) para OCO
            try:
                bal = self.exchange.fetch_balance()
                base = symbol.split("/")[0]
                actual_qty = bal["free"].get(base, filled_qty)
                actual_qty_str = self.exchange.amount_to_precision(symbol, actual_qty)
                sell_qty = float(actual_qty_str)
                if sell_qty <= 0:
                    sell_qty = self.exchange.amount_to_precision(symbol, filled_qty)
                    sell_qty = float(sell_qty) if isinstance(sell_qty, str) else sell_qty
            except Exception:
                sell_qty = filled_qty
            logger.info(f"  {symbol}: qty para OCO: {sell_qty} (comprado: {filled_qty})")

            # OCO: TP + SL — um cancela o outro, com retry
            oco_order = None
            import time as _time
            for attempt in range(3):
                try:
                    oco_qty_str = self.exchange.amount_to_precision(symbol, sell_qty)
                    # Garantir precos validos (minimo 1 tick, arredondado pela exchange)
                    tp_str = self.exchange.price_to_precision(symbol, tp_price)
                    sl_str = self.exchange.price_to_precision(symbol, sl_price)
                    # Se SL ou TP forem invalidos (ex: < tick), usa valores seguros
                    if float(tp_str) <= float(sl_str) or float(sl_str) <= 0:
                        raise ValueError("SL/TP inválidos após formatação")
                    oco_order = self.exchange.private_post_order_oco({
                        "symbol": symbol.replace("/", ""),
                        "side": "SELL",
                        "quantity": oco_qty_str,
                        "price": tp_str,
                        "stopPrice": sl_str,
                        "stopLimitPrice": sl_str,
                        "stopLimitTimeInForce": "GTC",
                    })
                    logger.info(f"  {symbol}: OCO SL={sl_price} TP={tp_price} orderListId={oco_order.get('orderListId')}")
                    break
                except Exception as e:
                    logger.error(f"  {symbol}: OCO falhou (tentativa {attempt+1}/3): {e}")
                    if attempt < 2:
                        _time.sleep(1)
                    else:
                        # 3 falhas: vender na hora pra não ficar exposto
                        logger.error(f"  {symbol}: OCO falhou 3x — vendendo posição imediatamente!")
                        try:
                            qty_str = self.exchange.amount_to_precision(symbol, sell_qty)
                            self.exchange.create_order(symbol, "market", "sell", qty_str)
                            logger.info(f"  {symbol}: vendido a mercado após falha do OCO")
                        except Exception as sell_err:
                            logger.error(f"  {symbol}: erro ao vender: {sell_err}")
                        oco_order = None

            # Persiste no KV store
            pos_data = {"symbol": symbol, "quantity": sell_qty, "entry_price": filled_price,
                         "sl_price": sl_price, "tp_price": tp_price, "entry_time": _time.time()}
            await self.kv.put(symbol.replace("/", "_"), json.dumps(pos_data).encode())

            return {"symbol": symbol, "status": "executed", "quantity": sell_qty,
                    "entry_price": filled_price, "sl_price": sl_price, "tp_price": tp_price,
                    "tier": order.get("tier"), "strategy": order.get("strategy"),
                    "score": order.get("score"), "rsi": order.get("rsi"),
                    "direction": order.get("direction", "LONG"),
                    "buy_order_id": buy_order.get("id"),
                    "oco_order_id": oco_order.get("orderListId") if oco_order else None}

        except Exception as e:
            logger.error(f"  {symbol}: erro ao executar ordem: {e}")
            return None

    async def process_orders(self, msg):
        try:
            orders = json.loads(msg.data.decode())
            logger.info(f"Processando {len(orders)} ordens (dry_run={DRY_RUN})")
            results = []

            for order in orders:
                result = await self.execute_order(order)
                if result:
                    results.append(result)

            if results:
                payload = json.dumps(results).encode()
                await self.js.publish("trade.executed", payload)
                logger.info(f"Publicadas {len(results)} execuções em trade.executed")

            await msg.ack()
        except Exception as e:
            logger.error(f"Erro ao processar: {e}")

    async def run(self):
        await self.connect_nats()
        await self.js.subscribe("trade.order", durable="EXECUTION_WORKER",
                                 cb=self.process_orders, manual_ack=True,
                                 config=ConsumerConfig(ack_wait=30))
        mode = "DRY RUN" if DRY_RUN else "PRODUÇÃO REAL"
        logger.info(f"fb-execution online [{mode}] (max_positions={MAX_POSITIONS})")
        while True:
            if self.nc.is_closed:
                await self.connect_nats()
            await asyncio.sleep(10)


if __name__ == "__main__":
    engine = ExecutionEngine()
    asyncio.run(engine.run())

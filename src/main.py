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
        self.exchange = ccxt.binance({
            "apiKey": BINANCE_API_KEY,
            "secret": BINANCE_API_SECRET,
            "enableRateLimit": True,
        })
        self.open_positions = {}  # symbol -> order_info

    async def connect_nats(self):
        self.nc = await nats.connect(NATS_URL)
        self.js = self.nc.jetstream()
        logger.info(f"NATS conectado: {NATS_URL}")

    async def get_open_positions(self):
        """Verifica posições já abertas na conta."""
        try:
            balance = self.exchange.fetch_balance()
            positions = {}
            for asset, info in balance["total"].items():
                if info > 0 and asset != "USDT":
                    # Verifica se é um ativo que temos (ignora dust)
                    if info * (balance.get(asset, {}).get("USDT", 0) or 1) > 5:  # min 5 USDT
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

        base = symbol.split("/")[0]

        # Verifica se já tem posição aberta
        if base in self.open_positions:
            logger.info(f"  {symbol}: já tem posição aberta → ignora")
            return None

        # Verifica máximo de posições
        active_count = len(self.open_positions)
        if active_count >= MAX_POSITIONS:
            logger.info(f"  {symbol}: max posições ({MAX_POSITIONS}) atingido → ignora")
            return None

        if DRY_RUN:
            logger.info(f"  [DRY RUN] {symbol}: BUY {quantity} @ ~{entry_price} SL={sl_price} TP={tp_price}")
            self.open_positions[base] = {
                "symbol": symbol,
                "quantity": quantity,
                "entry_price": entry_price,
                "sl_price": sl_price,
                "tp_price": tp_price,
            }
            return {
                "symbol": symbol,
                "status": "dry_run",
                "quantity": quantity,
                "entry_price": entry_price,
                "sl_price": sl_price,
                "tp_price": tp_price,
            }

        try:
            # 1. Market BUY
            logger.info(f"  {symbol}: executando market BUY {quantity}...")
            buy_order = self.exchange.create_order(symbol, "market", "buy", quantity)
            filled_price = float(buy_order.get("average", buy_order.get("price", entry_price)))
            filled_qty = float(buy_order.get("filled", quantity))
            logger.info(f"  {symbol}: BUY executado {filled_qty} @ {filled_price}")

            # 2. Stop-Limit SELL (Stop Loss)
            sl_trigger = round(sl_price * 1.001, 4)  # trigger ligeiramente acima do SL
            try:
                sl_order = self.exchange.create_order(
                    symbol, "stop_loss_limit", "sell", filled_qty, sl_price,
                    {"stopPrice": sl_trigger}
                )
                logger.info(f"  {symbol}: SL stop-limit @ {sl_price} (trigger={sl_trigger})")
            except Exception as e:
                logger.error(f"  {symbol}: erro ao criar SL: {e}")
                sl_order = None

            # 3. Limit SELL (Take Profit)
            try:
                tp_order = self.exchange.create_order(
                    symbol, "limit", "sell", filled_qty, tp_price
                )
                logger.info(f"  {symbol}: TP limit @ {tp_price}")
            except Exception as e:
                logger.error(f"  {symbol}: erro ao criar TP: {e}")
                tp_order = None

            self.open_positions[base] = {
                "symbol": symbol,
                "quantity": filled_qty,
                "entry_price": filled_price,
                "sl_price": sl_price,
                "tp_price": tp_price,
            }

            return {
                "symbol": symbol,
                "status": "executed",
                "quantity": filled_qty,
                "entry_price": filled_price,
                "sl_price": sl_price,
                "tp_price": tp_price,
                "buy_order_id": buy_order.get("id"),
                "sl_order_id": sl_order.get("id") if sl_order else None,
                "tp_order_id": tp_order.get("id") if tp_order else None,
            }

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

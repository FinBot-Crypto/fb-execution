import time
import logging
import os
import json
import redis
import ccxt

# Configuração de Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("execution")

# Configurações via Ambiente
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
DRY_RUN = os.getenv("DRY_RUN", "true").lower() == "true"

class ExecutionService:
    def __init__(self):
        self.r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
        self.pubsub = self.r.pubsub()
        # Aqui seriam as chaves reais do .env
        self.exchange = ccxt.binance({
            'apiKey': os.getenv('BINANCE_API_KEY'),
            'secret': os.getenv('BINANCE_API_SECRET'),
            'enableRateLimit': True,
        })

    def execute_trade(self, order_data):
        """Executa a ordem na exchange."""
        symbol = order_data['symbol']
        amount = order_data['position_value'] / order_data['entry_price']
        
        logger.info(f"EXECUTANDO ORDEM: {symbol} | Quantidade: {amount:.4f}")
        
        if DRY_RUN:
            logger.info(f"[DRY RUN] Ordem simulada com sucesso para {symbol}")
            return {"id": "mock_id_" + str(time.time()), "status": "closed"}
        
        try:
            # Ordem a Mercado (Exemplo)
            order = self.exchange.create_market_buy_order(symbol, amount)
            # Criar Stop e TP (Opcional, pode ser feito via OCO ou ordens separadas)
            return order
        except Exception as e:
            logger.error(f"Erro fatal na execução de {symbol}: {e}")
            return None

    def process_validated_risk(self, message):
        """Recebe ordens validadas pelo risco e executa."""
        orders = json.loads(message['data'])
        
        for order_data in orders:
            result = self.execute_trade(order_data)
            
            if result:
                execution_event = {
                    **order_data,
                    "exchange_order_id": result.get('id'),
                    "executed_at": time.time(),
                    "status": "executed"
                }
                
                payload = json.dumps(execution_event)
                self.r.publish("events:trade_executed", payload)
                # Salva no registro de posições abertas
                self.r.hset("positions:active", order_data['symbol'], payload)

    def run(self):
        self.pubsub.subscribe(**{'events:risk_validated': self.process_validated_risk})
        logger.info(f"Execution Service rodando (DRY_RUN={DRY_RUN}) - Aguardando 'events:risk_validated'...")
        
        for message in self.pubsub.listen():
            if message['type'] == 'message':
                pass

if __name__ == "__main__":
    service = ExecutionService()
    service.run()

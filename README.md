# fb-execution

Executa ordens na Binance (market buy + SL/TP).

## Fluxo

```
trade.order (fb-trade-decision)
  → fb-execution
    → verifica max posições (20)
    → verifica se já tem posição no ativo
    → market BUY na Binance
    → stop-limit SELL (SL)
    → limit SELL (TP)
    → trade.executed
```

## Variáveis de Ambiente

| Variável | Padrão | Descrição |
|----------|--------|-----------|
| `NATS_URL` | `nats://crypto-nats:4222` | Servidor NATS |
| `DRY_RUN` | `true` | `true` = simulação, `false` = ordens reais |
| `MAX_POSITIONS` | `20` | Máximo de posições simultâneas |
| `BINANCE_API_KEY` | | Chave API Binance |
| `BINANCE_API_SECRET` | | Secret API Binance |

## Deploy

```bash
docker run -e NATS_URL=nats://crypto-nats:4222 \
  -e DRY_RUN=true \
  -e BINANCE_API_KEY=xxx -e BINANCE_API_SECRET=xxx \
  fb-execution:latest
```

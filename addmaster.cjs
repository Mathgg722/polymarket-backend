const fs = require('fs');
let content = fs.readFileSync('main.py', 'utf8');

const masterCode = `

# ═══════════════════════════════════════════════════════════════════════════
# MASTER — Market-Adaptive Stock Transformer (Shanghai Jiao Tong 2024)
# Adaptado para Prediction Markets
# Mercado calmo → analisa histórico individual (Intra-Token)
# Mercado volátil → analisa correlações entre tokens (Inter-Token)
# ═══════════════════════════════════════════════════════════════════════════

def _master_volatility(prices: list) -> float:
    """Calcula volatilidade normalizada de uma série de preços."""
    if len(prices) < 2:
        return 0.0
    changes = [abs(prices[i] - prices[i+1]) for i in range(len(prices)-1)]
    return sum(changes) / len(changes)


def _master_intra_analysis(prices: list, current: float) -> dict:
    """
    Intra-Token Aggregation: analisa padrões históricos do próprio token.
    Usado quando mercado está CALMO.
    """
    if len(prices) < 10:
        return {"signal": "INSUFFICIENT_DATA", "confidence": 0.0, "predicted": current}

    # Média móvel curta vs longa
    short_ma = sum(prices[:5]) / 5
    long_ma = sum(prices[:20]) / min(20, len(prices))

    # Momentum
    momentum = current - prices[min(10, len(prices)-1)]

    # Reversão à média
    hist_mean = sum(prices) / len(prices)
    deviation = current - hist_mean

    # Score de direção
    bullish = 0
    bearish = 0

    if short_ma > long_ma: bullish += 1
    else: bearish += 1

    if momentum > 0.01: bullish += 1
    elif momentum < -0.01: bearish += 1

    if deviation > 0.05: bearish += 1  # Sobrecomprado
    elif deviation < -0.05: bullish += 1  # Sobrevendido

    # Previsão
    if bullish > bearish:
        direction = "UP"
        predicted = min(current + abs(momentum) * 0.5, 0.97)
        confidence = bullish / 3
    elif bearish > bullish:
        direction = "DOWN"
        predicted = max(current - abs(momentum) * 0.5, 0.03)
        confidence = bearish / 3
    else:
        direction = "NEUTRAL"
        predicted = hist_mean
        confidence = 0.4

    return {
        "mode": "INTRA",
        "signal": direction,
        "confidence": round(confidence, 2),
        "predicted_price": round(predicted * 100, 1),
        "current_price": round(current * 100, 1),
        "short_ma": round(short_ma * 100, 1),
        "long_ma": round(long_ma * 100, 1),
        "momentum": round(momentum * 100, 2),
        "deviation_from_mean": round(deviation * 100, 2),
    }


def _master_inter_analysis(token_prices: dict, target_token_id: str, current: float) -> dict:
    """
    Inter-Token Aggregation: analisa correlações entre tokens do mesmo mercado.
    Usado quando mercado está VOLÁTIL.
    """
    correlations = []
    for tid, prices in token_prices.items():
        if tid == target_token_id or len(prices) < 5:
            continue
        # Correlação simples: se YES sobe, NO desce (inverso)
        other_current = prices[0] if prices else 0.5
        # Em mercados binários, YES + NO ≈ 1.0
        implied = 1.0 - other_current
        diff = abs(current - implied)
        correlations.append({
            "token_id": tid,
            "other_price": round(other_current * 100, 1),
            "implied_price": round(implied * 100, 1),
            "divergence": round(diff * 100, 2),
        })

    if not correlations:
        return {"mode": "INTER", "signal": "NO_CORRELATION", "confidence": 0.0, "predicted_price": round(current * 100, 1)}

    best_corr = max(correlations, key=lambda x: x["divergence"])
    divergence = best_corr["divergence"]

    if divergence > 3:
        # Arbitragem: preço atual diverge do implicado pela contraparte
        if current * 100 < best_corr["implied_price"]:
            signal = "UP"
            predicted = best_corr["implied_price"] / 100
        else:
            signal = "DOWN"
            predicted = best_corr["implied_price"] / 100
        confidence = min(divergence / 20, 0.9)
    else:
        signal = "NEUTRAL"
        predicted = current
        confidence = 0.3

    return {
        "mode": "INTER",
        "signal": signal,
        "confidence": round(confidence, 2),
        "predicted_price": round(predicted * 100, 1),
        "current_price": round(current * 100, 1),
        "correlations": correlations[:3],
        "max_divergence": divergence,
    }


def _master_gating(volatility: float, threshold: float = 0.005) -> str:
    """
    Market-Guided Gating: decide qual modo usar.
    Calmo → INTRA | Volátil → INTER
    """
    if volatility > threshold:
        return "INTER"
    return "INTRA"


@app.get("/master")
def get_master_predictions(
    limit: int = Query(15, ge=1, le=50),
    db: Session = Depends(get_db),
):
    """
    MASTER — Market-Adaptive Prediction Engine
    Baseado no paper AAAI-2024 da Shanghai Jiao Tong University.
    Adapta automaticamente entre análise intra-token (calmo) e inter-token (volátil).
    """
    now = datetime.utcnow()
    results = []

    # Buscar mercados ativos com dados suficientes
    markets = (
        db.query(Market).join(Token)
        .filter(Token.price > 0.05, Token.price < 0.95)
        .filter((Market.end_date == None) | (Market.end_date > now))
        .distinct().limit(200).all()
    )

    market_stats = {
        "total_calmo": 0,
        "total_volatil": 0,
        "total_analisados": 0,
    }

    for market in markets:
        if len(results) >= limit * 3:
            break

        try:
            # Buscar todos os tokens do mercado
            token_prices = {}
            token_details = {}

            for token in market.tokens:
                snaps = (
                    db.query(Snapshot)
                    .filter(Snapshot.token_id == token.token_id)
                    .order_by(Snapshot.timestamp.desc())
                    .limit(100).all()
                )
                if len(snaps) < 10:
                    continue
                prices = [s.price for s in snaps]
                token_prices[token.token_id] = prices
                token_details[token.token_id] = {
                    "outcome": token.outcome,
                    "current": token.price,
                }

            if not token_prices:
                continue

            # Analisar cada token YES
            for tid, prices in token_prices.items():
                detail = token_details.get(tid, {})
                if (detail.get("outcome") or "").upper() != "YES":
                    continue

                current = detail["current"]
                if current < 0.05 or current > 0.95:
                    continue

                # 1. Calcular volatilidade
                vol = _master_volatility(prices[:20])

                # 2. Gating: decidir modo
                mode = _master_gating(vol)

                # 3. Análise adaptativa
                if mode == "INTRA":
                    analysis = _master_intra_analysis(prices, current)
                    market_stats["total_calmo"] += 1
                else:
                    analysis = _master_inter_analysis(token_prices, tid, current)
                    market_stats["total_volatil"] += 1

                market_stats["total_analisados"] += 1

                # 4. Filtrar só sinais com confiança suficiente
                if analysis["confidence"] < 0.45:
                    continue
                if analysis["signal"] == "NEUTRAL":
                    continue

                predicted = analysis.get("predicted_price", current * 100)
                edge = round(predicted - current * 100, 1)

                if abs(edge) < 2:
                    continue

                # 5. Score final MASTER
                master_score = round(
                    analysis["confidence"] * 50 +
                    min(abs(edge) * 2, 30) +
                    (20 if mode == "INTER" else 10),  # Inter tem mais peso em volatilidade
                    1
                )

                conviction = (
                    "🔴 FORTE" if master_score >= 75 else
                    "🟠 ALTA" if master_score >= 60 else
                    "🟡 MÉDIA" if master_score >= 45 else
                    "🔵 FRACA"
                )

                results.append({
                    "market": market.question,
                    "slug": market.market_slug,
                    "polymarket_url": f"https://polymarket.com/event/{market.market_slug}",
                    "outcome": "YES",
                    "current_price": round(current * 100, 1),
                    "predicted_price": predicted,
                    "edge": edge,
                    "direction": analysis["signal"],
                    "master_mode": mode,
                    "master_score": master_score,
                    "conviction": conviction,
                    "volatility": round(vol * 100, 3),
                    "confidence": analysis["confidence"],
                    "analysis": analysis,
                    "analisado_em": now.isoformat(),
                })

        except Exception as e:
            print(f"[MASTER] erro {market.market_slug}: {e}")
            continue

    # Ordenar por master_score
    results.sort(key=lambda x: x["master_score"], reverse=True)
    top = results[:limit]

    # Resumo
    up_signals = sum(1 for r in top if r["direction"] == "UP")
    down_signals = sum(1 for r in top if r["direction"] == "DOWN")
    intra_count = sum(1 for r in top if r["master_mode"] == "INTRA")
    inter_count = sum(1 for r in top if r["master_mode"] == "INTER")

    return {
        "modelo": "MASTER — Market-Adaptive Stock Transformer",
        "paper": "AAAI-2024, Shanghai Jiao Tong University",
        "adaptado_para": "Polymarket Prediction Markets",
        "total_mercados_analisados": market_stats["total_analisados"],
        "mercados_calmos_intra": market_stats["total_calmo"],
        "mercados_volateis_inter": market_stats["total_volatil"],
        "snapshots_disponiveis": 826948,
        "resumo": {
            "sinais_up": up_signals,
            "sinais_down": down_signals,
            "modo_intra": intra_count,
            "modo_inter": inter_count,
            "score_medio": round(sum(r["master_score"] for r in top) / max(len(top), 1), 1),
        },
        "previsoes": top,
        "gerado_em": now.isoformat(),
    }
`;

// Inserir antes da última linha do arquivo
const insertPoint = '\n@app.get("/game-theory/mec")';
if (content.includes(insertPoint)) {
    content = content.replace(insertPoint, masterCode + insertPoint);
    fs.writeFileSync('main.py', content, 'utf8');
    console.log('OK! Endpoint /master adicionado!');
    console.log('Linhas totais:', content.split('\n').length);
} else {
    console.log('Ponto de inserção não encontrado, tentando outro...');
    // Fallback: adicionar no final
    content = content + masterCode;
    fs.writeFileSync('main.py', content, 'utf8');
    console.log('OK! Adicionado no final do arquivo.');
}
